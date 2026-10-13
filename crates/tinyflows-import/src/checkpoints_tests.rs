use super::*;
use crate::test_support::{docs, expected, fixtures, json, tree_digest};
use serde_json::json;
use tinystoragedrivers_sqlite::rusqlite::Connection;

fn target() -> DriverCheckpointer<Value> {
    DriverCheckpointer::new(docs())
}

fn sqlite_db() -> std::path::PathBuf {
    fixtures().join("checkpoints/checkpoints.db")
}

fn file_dir() -> std::path::PathBuf {
    fixtures().join("checkpoints/file")
}

fn c2() -> CheckpointConfig {
    CheckpointConfig {
        thread_id: "run-1".into(),
        checkpoint_id: Some("c2".into()),
        namespace: vec![],
    }
}

/// Both legacy backends held the same data: whole threads, namespaced
/// subgraph checkpoints, a thread whose id needed escaping, and writes.
async fn assert_fixture_data_came_across(checkpointer: &DriverCheckpointer<Value>) {
    let expected = expected();
    let expected = &expected["checkpoints"];
    for thread in ["run-1", "Run Two/Upper"] {
        let imported = checkpointer.get_thread(thread).await.unwrap();
        assert_eq!(
            Value::Array(imported.iter().map(json).collect()),
            expected["threads"][thread],
            "thread {thread}"
        );
    }
    let writes = checkpointer.get_writes(&c2()).await.unwrap();
    assert_eq!(
        Value::Array(writes.iter().map(json).collect()),
        expected["writes"]
    );
    // The subgraph's checkpoint stays in its own namespace.
    let sub = checkpointer
        .get_scoped("run-1", None, &["sub".to_string()])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(sub.checkpoint_id, "s1");
    let root = checkpointer.get("run-1", None).await.unwrap().unwrap();
    assert_eq!(root.checkpoint_id, "s1", "the latest written, as before");
    let lineage = checkpointer
        .state_history("run-1", &[], None)
        .await
        .unwrap();
    assert_eq!(
        lineage
            .iter()
            .map(|t| t.checkpoint.checkpoint_id.as_str())
            .collect::<Vec<_>>(),
        ["c2", "c1"]
    );
}

#[tokio::test]
async fn sqlite_checkpoints_and_writes_come_across_in_order() {
    let checkpointer = target();
    let report = import_sqlite_checkpoints(&sqlite_db(), &checkpointer)
        .await
        .unwrap();
    assert!(report.is_clean(), "{:?}", report.problems());
    assert_eq!(report.tally("checkpoints").written, 4);
    assert_eq!(report.tally("writes").written, 3);
    assert_fixture_data_came_across(&checkpointer).await;
}

#[tokio::test]
async fn file_checkpoints_and_writes_come_across_and_a_torn_last_line_is_forgiven() {
    let checkpointer = target();
    let report = import_file_checkpoints(&file_dir(), &checkpointer)
        .await
        .unwrap();
    assert!(report.is_clean(), "{:?}", report.problems());
    assert_eq!(report.tally("checkpoints").written, 4);
    assert_eq!(report.tally("writes").written, 3);
    assert_fixture_data_came_across(&checkpointer).await;
    // The torn line is real: the file does not end in a newline.
    let torn = std::fs::read_to_string(file_dir().join("run-1.jsonl")).unwrap();
    assert!(!torn.ends_with('\n'));
}

#[tokio::test]
async fn importing_again_writes_nothing_and_keeps_each_thread_in_order() {
    let checkpointer = target();
    let first = import_sqlite_checkpoints(&sqlite_db(), &checkpointer)
        .await
        .unwrap();
    let second = import_sqlite_checkpoints(&sqlite_db(), &checkpointer)
        .await
        .unwrap();
    assert_eq!(second.total_written(), 0);
    assert_eq!(
        second.tally("checkpoints").existing,
        first.tally("checkpoints").written
    );
    assert_eq!(
        second.tally("writes").existing,
        first.tally("writes").written
    );
    assert_fixture_data_came_across(&checkpointer).await;
}

#[tokio::test]
async fn the_second_backend_adds_nothing_when_the_first_already_brought_the_same_data() {
    let checkpointer = target();
    import_sqlite_checkpoints(&sqlite_db(), &checkpointer)
        .await
        .unwrap();
    let from_files = import_file_checkpoints(&file_dir(), &checkpointer)
        .await
        .unwrap();
    assert_eq!(from_files.total_written(), 0);
    assert_fixture_data_came_across(&checkpointer).await;
}

#[tokio::test]
async fn the_legacy_files_are_left_exactly_as_they_were() {
    let before = tree_digest(&fixtures());
    import_sqlite_checkpoints(&sqlite_db(), &target())
        .await
        .unwrap();
    import_file_checkpoints(&file_dir(), &target())
        .await
        .unwrap();
    assert_eq!(tree_digest(&fixtures()), before);
}

#[tokio::test]
async fn missing_stores_are_empty_reports() {
    let dir = tempfile::tempdir().unwrap();
    let checkpointer = target();
    assert_eq!(
        import_sqlite_checkpoints(&dir.path().join("none.db"), &checkpointer)
            .await
            .unwrap()
            .total_written(),
        0
    );
    assert_eq!(
        import_file_checkpoints(&dir.path().join("none"), &checkpointer)
            .await
            .unwrap()
            .total_written(),
        0
    );
}

#[tokio::test]
async fn a_database_without_the_tables_imports_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("checkpoints.db");
    Connection::open(&path)
        .unwrap()
        .execute_batch("CREATE TABLE thread_leases (thread_id TEXT PRIMARY KEY, owner TEXT, expires_at INTEGER);
                        INSERT INTO thread_leases VALUES ('t', 'o', 1);")
        .unwrap();
    let report = import_sqlite_checkpoints(&path, &target()).await.unwrap();
    assert_eq!(
        report.total_written(),
        0,
        "leases are runtime state, not data"
    );
}

#[tokio::test]
async fn rows_that_do_not_decode_cost_only_themselves() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("checkpoints.db");
    let good =
        serde_json::to_string(&crate::checkpoints::tests::sample("t", "c1", None, &[])).unwrap();
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch(
        "CREATE TABLE checkpoints (seq INTEGER PRIMARY KEY AUTOINCREMENT, record TEXT NOT NULL);
         CREATE TABLE checkpoint_writes (thread_id TEXT NOT NULL, namespace TEXT NOT NULL, checkpoint_id TEXT NOT NULL,
             task_id TEXT NOT NULL, idx INTEGER NOT NULL, node TEXT NOT NULL, channel TEXT NOT NULL, payload TEXT NOT NULL);",
    )
    .unwrap();
    conn.execute("INSERT INTO checkpoints (record) VALUES (?1)", [&good])
        .unwrap();
    conn.execute("INSERT INTO checkpoints (record) VALUES ('{broken')", [])
        .unwrap();
    for (namespace, payload) in [("[]", "{\"ok\":1}"), ("not json", "1"), ("[]", "not json")] {
        conn.execute(
            "INSERT INTO checkpoint_writes VALUES ('t', ?1, 'c1', 'task', 0, 'n', 'ch', ?2)",
            [namespace, payload],
        )
        .unwrap();
    }
    drop(conn);
    let checkpointer = target();
    let report = import_sqlite_checkpoints(&path, &checkpointer)
        .await
        .unwrap();
    assert_eq!(report.tally("checkpoints").written, 1);
    assert_eq!(report.tally("checkpoints").invalid, 1);
    assert_eq!(report.tally("writes").written, 1);
    assert_eq!(report.tally("writes").invalid, 2);
}

mod files {
    use super::*;

    fn line(thread: &str, id: &str, parent: Option<&str>) -> String {
        serde_json::to_string(&sample(thread, id, parent, &[])).unwrap()
    }

    #[tokio::test]
    async fn a_bad_line_in_the_middle_is_counted_and_the_rest_imported() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("t.jsonl"),
            format!(
                "{}\nnot json\n\n{}\n",
                line("t", "c1", None),
                line("t", "c2", Some("c1"))
            ),
        )
        .unwrap();
        let checkpointer = target();
        let report = import_file_checkpoints(dir.path(), &checkpointer)
            .await
            .unwrap();
        assert_eq!(report.tally("checkpoints").written, 2);
        assert_eq!(report.tally("checkpoints").invalid, 1);
        let ids: Vec<_> = checkpointer
            .get_thread("t")
            .await
            .unwrap()
            .into_iter()
            .map(|c| c.checkpoint_id)
            .collect();
        assert_eq!(ids, ["c1", "c2"]);
    }

    #[tokio::test]
    async fn a_file_that_is_complete_but_not_a_record_is_invalid_not_torn() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("t.jsonl"), "{\"checkpoint_id\":\"x\"}\n").unwrap();
        let report = import_file_checkpoints(dir.path(), &target())
            .await
            .unwrap();
        assert_eq!(report.tally("checkpoints").invalid, 1);
    }

    #[tokio::test]
    async fn an_existing_checkpoint_and_an_existing_control_plane_write_are_not_overwritten() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("t.jsonl"),
            format!("{}\n", line("t", "c1", None)),
        )
        .unwrap();
        let write = |payload: Value| {
            json!({
                "namespace": [],
                "checkpoint_id": "c1",
                "write": { "node": "n", "task_id": "task", "idx": -1, "channel": "resume", "payload": payload },
            })
            .to_string()
        };
        std::fs::write(
            dir.path().join("t.writes.jsonl"),
            format!("{}\n", write(json!("legacy"))),
        )
        .unwrap();

        let checkpointer = target();
        let mut live = sample("t", "c1", None, &[]);
        live.state = json!({ "live": true });
        checkpointer.put(live).await.unwrap();
        checkpointer
            .put_writes(
                &CheckpointConfig {
                    thread_id: "t".into(),
                    checkpoint_id: Some("c1".into()),
                    namespace: vec![],
                },
                &[PendingWrite {
                    node: NodeId::new("n"),
                    task_id: "task".into(),
                    idx: -1,
                    channel: "resume".into(),
                    payload: json!("live"),
                }],
            )
            .await
            .unwrap();

        let report = import_file_checkpoints(dir.path(), &checkpointer)
            .await
            .unwrap();

        assert_eq!(report.total_written(), 0);
        assert_eq!(report.tally("checkpoints").existing, 1);
        assert_eq!(report.tally("writes").existing, 1);
        let kept = checkpointer.get("t", Some("c1")).await.unwrap().unwrap();
        assert_eq!(kept.state, json!({ "live": true }));
        let writes = checkpointer
            .get_writes(&CheckpointConfig {
                thread_id: "t".into(),
                checkpoint_id: Some("c1".into()),
                namespace: vec![],
            })
            .await
            .unwrap();
        assert_eq!(writes[0].payload, json!("live"));
    }

    #[tokio::test]
    async fn writes_sidecar_names_are_unescaped_both_ways_and_bad_ones_are_invalid() {
        let dir = tempfile::tempdir().unwrap();
        let write = json!({
            "checkpoint_id": "c1",
            "write": { "node": "n", "task_id": "task", "idx": 0, "channel": "ch", "payload": 1 },
        })
        .to_string();
        // Current escaping (`%52` is `R`), the older kind that left capitals alone,
        // a malformed escape, and bytes that are not UTF-8.
        for name in ["%52un%2F1", "Alice", "bad%ZZ", "short%4", "%FF%FE"] {
            std::fs::write(
                dir.path().join(format!("{name}.writes.jsonl")),
                format!("{write}\n"),
            )
            .unwrap();
        }
        let checkpointer = target();
        let report = import_file_checkpoints(dir.path(), &checkpointer)
            .await
            .unwrap();
        assert_eq!(report.tally("writes").written, 2);
        assert_eq!(report.tally("writes").invalid, 3);
        for thread in ["Run/1", "Alice"] {
            let config = CheckpointConfig {
                thread_id: thread.into(),
                checkpoint_id: Some("c1".into()),
                namespace: vec![],
            };
            assert_eq!(
                checkpointer.get_writes(&config).await.unwrap().len(),
                1,
                "{thread}"
            );
        }
    }

    #[tokio::test]
    async fn things_that_are_not_checkpoint_files_are_ignored() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("README.txt"), "x").unwrap();
        std::fs::write(dir.path().join("t.jsonl.tmp.1.2"), "not a record").unwrap();
        std::fs::create_dir_all(dir.path().join("sub.jsonl")).unwrap();
        let report = import_file_checkpoints(dir.path(), &target())
            .await
            .unwrap();
        assert_eq!(report.total_written(), 0);
        assert!(report.is_clean());
    }

    #[tokio::test]
    async fn a_path_that_is_not_a_directory_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("file");
        std::fs::write(&file, "x").unwrap();
        assert!(import_file_checkpoints(&file, &target()).await.is_err());
    }
}

#[test]
fn escaping_round_trips_the_way_the_old_stores_wrote_it() {
    assert_eq!(
        unescape_thread_id("plain-1.x_y").as_deref(),
        Some("plain-1.x_y")
    );
    assert_eq!(unescape_thread_id("a%20b%2Fc").as_deref(), Some("a b/c"));
    assert_eq!(unescape_thread_id("%C3%A9").as_deref(), Some("é"));
    assert_eq!(unescape_thread_id("").as_deref(), Some(""));
    assert!(unescape_thread_id("%").is_none());
    assert!(unescape_thread_id("%G1").is_none());
    assert!(unescape_thread_id("%C3").is_none(), "half a UTF-8 sequence");
}

/// A checkpoint of `thread` in `namespace`, for the files these tests write.
pub(super) fn sample(
    thread: &str,
    id: &str,
    parent: Option<&str>,
    namespace: &[&str],
) -> Checkpoint<Value> {
    Checkpoint {
        thread_id: thread.to_string(),
        checkpoint_id: id.to_string(),
        run_id: Some("run-1".to_string()),
        parent_checkpoint_id: parent.map(str::to_string),
        namespace: namespace.iter().map(|s| s.to_string()).collect(),
        state: json!({ "id": id }),
        next_nodes: vec![NodeId::new("next")],
        completed_tasks: vec![NodeId::new("done")],
        pending_writes: Vec::new(),
        interrupts: Vec::new(),
        pending_activations: None,
        barrier_arrivals: Vec::new(),
        metadata: json!({ "source": "loop", "step": 1 }),
    }
}
