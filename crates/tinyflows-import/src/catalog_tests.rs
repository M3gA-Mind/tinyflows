use super::*;
use crate::test_support::{docs, expected, fixtures, json, sorted_by, tree_digest};
use serde_json::json;
use tinystoragedrivers_sqlite::rusqlite::Connection;

fn catalog_dir() -> std::path::PathBuf {
    fixtures().join("catalog")
}

fn target() -> FlowCatalogDocuments {
    FlowCatalogDocuments::new(docs())
}

#[tokio::test]
async fn the_whole_catalog_comes_across_with_its_ids_and_every_field() {
    let catalog = target();
    let report = import_flow_catalog(&catalog_dir(), &catalog).await.unwrap();
    assert!(report.is_clean(), "{:?}", report.problems());
    let expected = expected();
    let expected = &expected["catalog"];

    // Flows.
    let (flows, skipped) = catalog.list_flows().await.unwrap();
    assert_eq!(skipped, 0);
    assert_eq!(
        sorted_by(flows.iter().map(json).collect(), "id"),
        sorted_by(expected["flows"].as_array().unwrap().clone(), "id")
    );
    // Revisions and runs, per flow.
    for flow in &flows {
        let revisions = catalog.list_revisions(&flow.id, 100).await.unwrap();
        assert_eq!(
            sorted_by(revisions.iter().map(json).collect(), "id"),
            sorted_by(expected["revisions"][&flow.id].as_array().unwrap().clone(), "id"),
            "revisions of {}",
            flow.id
        );
        let runs = catalog.list_flow_runs(&flow.id, 100).await.unwrap();
        assert_eq!(
            sorted_by(runs.iter().map(json).collect(), "id"),
            sorted_by(expected["runs"][&flow.id].as_array().unwrap().clone(), "id"),
            "runs of {}",
            flow.id
        );
    }
    // State, suggestions, drafts.
    for entry in expected["kv"].as_array().unwrap() {
        let (namespace, key, value) = (
            entry[0].as_str().unwrap(),
            entry[1].as_str().unwrap(),
            &entry[2],
        );
        assert_eq!(
            catalog.kv_get(namespace, key).await.unwrap().as_ref(),
            Some(value),
            "{namespace}/{key}"
        );
    }
    let suggestions = catalog.list_suggestions(None, 100).await.unwrap();
    assert_eq!(
        sorted_by(suggestions.iter().map(json).collect(), "id"),
        sorted_by(expected["suggestions"].as_array().unwrap().clone(), "id")
    );
    let drafts = catalog.list_drafts().await.unwrap();
    assert_eq!(
        sorted_by(drafts.iter().map(json).collect(), "id"),
        sorted_by(expected["drafts"].as_array().unwrap().clone(), "id")
    );
}

#[tokio::test]
async fn the_report_counts_every_kind() {
    let report = import_flow_catalog(&catalog_dir(), &target()).await.unwrap();
    for (kind, written) in [
        ("flows", 2),
        ("revisions", 2),
        ("runs", 4),
        ("state", 3),
        ("suggestions", 2),
        ("drafts", 2),
    ] {
        assert_eq!(report.tally(kind).written, written, "{kind}");
        assert_eq!(report.tally(kind).existing, 0, "{kind}");
    }
    assert_eq!(report.total_written(), 15);
}

#[tokio::test]
async fn a_dismissed_suggestion_stays_dismissed_and_a_parked_run_stays_parked() {
    let catalog = target();
    import_flow_catalog(&catalog_dir(), &catalog).await.unwrap();
    let suggestions = catalog.list_suggestions(None, 100).await.unwrap();
    let dismissed = suggestions.iter().find(|s| s.id == "sugg-2").unwrap();
    assert_eq!(dismissed.status.as_str(), "dismissed");
    let parked = catalog.get_flow_run("run-parked").await.unwrap().unwrap();
    assert_eq!(parked.status, "pending_approval");
    assert_eq!(parked.pending_approvals, ["gate"]);
    assert_eq!(parked.graph_hash.as_deref(), Some("hash-1"));
    let live = catalog.get_flow_run("run-live").await.unwrap().unwrap();
    assert_eq!(live.status, "running");
}

#[tokio::test]
async fn importing_again_writes_nothing_and_finds_everything_already_there() {
    let catalog = target();
    let first = import_flow_catalog(&catalog_dir(), &catalog).await.unwrap();
    let second = import_flow_catalog(&catalog_dir(), &catalog).await.unwrap();
    assert_eq!(second.total_written(), 0);
    for (kind, tally) in first.kinds() {
        assert_eq!(second.tally(kind).existing, tally.written, "{kind}");
    }
}

#[tokio::test]
async fn the_legacy_files_are_left_exactly_as_they_were() {
    let before = tree_digest(&fixtures());
    import_flow_catalog(&catalog_dir(), &target()).await.unwrap();
    assert_eq!(tree_digest(&fixtures()), before);
}

#[tokio::test]
async fn nothing_the_target_already_holds_is_replaced() {
    let catalog = target();
    let live = catalog
        .create_flow(
            "live name".into(),
            serde_json::from_value(json!({ "nodes": [], "edges": [] })).unwrap(),
            false,
            true,
        )
        .await
        .unwrap();
    // The live flow takes the legacy flow's id.
    catalog.remove_flow(&live.id).await.unwrap();
    let mut owned = live.clone();
    owned.id = "flow-alpha".into();
    catalog.upsert_flow(&owned).await.unwrap();
    catalog.kv_set("workflow:flow-alpha", "cursor", &json!("mine")).await.unwrap();

    let report = import_flow_catalog(&catalog_dir(), &catalog).await.unwrap();

    assert_eq!(report.tally("flows").existing, 1);
    assert_eq!(report.tally("flows").written, 1);
    assert_eq!(catalog.get_flow("flow-alpha").await.unwrap().unwrap().name, "live name");
    assert_eq!(
        catalog.kv_get("workflow:flow-alpha", "cursor").await.unwrap(),
        Some(json!("mine"))
    );
    assert_eq!(report.tally("state").existing, 1);
}

#[tokio::test]
async fn a_directory_with_nothing_in_it_is_an_empty_report() {
    let dir = tempfile::tempdir().unwrap();
    let report = import_flow_catalog(dir.path(), &target()).await.unwrap();
    assert_eq!(report.total_written(), 0);
    assert!(report.is_clean());
    let report = import_flow_catalog(&dir.path().join("never-existed"), &target())
        .await
        .unwrap();
    assert_eq!(report.total_written(), 0);
}

fn legacy_db(dir: &std::path::Path, sql: &str) {
    Connection::open(dir.join("flows.db"))
        .unwrap()
        .execute_batch(sql)
        .unwrap();
}

const GRAPH: &str = r#"{"nodes":[{"id":"t","kind":"trigger","name":"start","config":{"trigger_kind":"manual"}}],"edges":[]}"#;

#[tokio::test]
async fn a_database_from_before_later_columns_reads_with_their_defaults() {
    let dir = tempfile::tempdir().unwrap();
    legacy_db(
        dir.path(),
        &format!(
            "CREATE TABLE flow_definitions (id TEXT PRIMARY KEY, name TEXT NOT NULL, graph_json TEXT NOT NULL,
                 enabled INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                 last_run_at TEXT, last_status TEXT);
             INSERT INTO flow_definitions VALUES ('old', 'Old', '{GRAPH}', 1, '2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z', NULL, NULL);
             CREATE TABLE flow_runs (id TEXT PRIMARY KEY, flow_id TEXT NOT NULL, thread_id TEXT NOT NULL, status TEXT NOT NULL,
                 started_at TEXT NOT NULL, finished_at TEXT, steps_json TEXT NOT NULL DEFAULT '[]',
                 pending_approvals_json TEXT NOT NULL DEFAULT '[]', error TEXT);
             INSERT INTO flow_runs VALUES ('r', 'old', 't', 'completed', '2024-01-01T00:00:00Z', '2024-01-01T00:00:01Z', '[]', '[]', NULL);"
        ),
    );
    let catalog = target();
    let report = import_flow_catalog(dir.path(), &catalog).await.unwrap();
    assert!(report.is_clean(), "{:?}", report.problems());
    let flow = catalog.get_flow("old").await.unwrap().unwrap();
    assert!(!flow.require_approval, "a column that did not exist is false");
    let run = catalog.get_flow_run("r").await.unwrap().unwrap();
    assert!(run.graph_hash.is_none());
}

#[tokio::test]
async fn a_row_that_does_not_decode_costs_only_itself_and_its_dependents() {
    let dir = tempfile::tempdir().unwrap();
    legacy_db(
        dir.path(),
        &format!(
            "CREATE TABLE flow_definitions (id TEXT PRIMARY KEY, name TEXT NOT NULL, graph_json TEXT NOT NULL,
                 enabled INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                 last_run_at TEXT, last_status TEXT, require_approval INTEGER NOT NULL DEFAULT 0);
             INSERT INTO flow_definitions VALUES ('good', 'Good', '{GRAPH}', 1, 'a', 'a', NULL, NULL, 0);
             INSERT INTO flow_definitions VALUES ('broken', 'Broken', 'not json', 1, 'b', 'b', NULL, NULL, 0);
             CREATE TABLE flow_revisions (id TEXT PRIMARY KEY, flow_id TEXT NOT NULL, graph_json TEXT NOT NULL,
                 name TEXT NOT NULL, require_approval INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL);
             INSERT INTO flow_revisions VALUES ('rev-ok', 'good', '{GRAPH}', 'Good', 0, 'c');
             INSERT INTO flow_revisions VALUES ('rev-orphan', 'broken', '{GRAPH}', 'Broken', 0, 'd');
             INSERT INTO flow_revisions VALUES ('rev-bad', 'good', 'nope', 'Good', 0, 'e');
             CREATE TABLE flow_runs (id TEXT PRIMARY KEY, flow_id TEXT NOT NULL, thread_id TEXT NOT NULL, status TEXT NOT NULL,
                 started_at TEXT NOT NULL, finished_at TEXT, steps_json TEXT NOT NULL DEFAULT '[]',
                 pending_approvals_json TEXT NOT NULL DEFAULT '[]', error TEXT, graph_hash TEXT);
             INSERT INTO flow_runs VALUES ('run-ok', 'good', 't', 'completed', 'a', NULL, '[]', '[]', NULL, NULL);
             INSERT INTO flow_runs VALUES ('run-orphan', 'broken', 't', 'completed', 'b', NULL, '[]', '[]', NULL, NULL);
             INSERT INTO flow_runs VALUES ('run-bad-steps', 'good', 't', 'completed', 'c', NULL, 'oops', '[]', NULL, NULL);
             INSERT INTO flow_runs VALUES ('run-bad-approvals', 'good', 't', 'completed', 'd', NULL, '[]', 'oops', NULL, NULL);
             CREATE TABLE flow_state (namespace TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY (namespace, key));
             INSERT INTO flow_state VALUES ('n', 'ok', '{{\"a\":1}}');
             INSERT INTO flow_state VALUES ('n', 'bad', 'not json');
             CREATE TABLE flow_suggestions (id TEXT PRIMARY KEY, title TEXT NOT NULL, one_liner TEXT NOT NULL, rationale TEXT NOT NULL,
                 trigger_hint TEXT, steps_json TEXT NOT NULL DEFAULT '[]', connections_json TEXT NOT NULL DEFAULT '[]',
                 slugs_json TEXT NOT NULL DEFAULT '[]', build_prompt TEXT NOT NULL, confidence REAL NOT NULL DEFAULT 0,
                 status TEXT NOT NULL DEFAULT 'new', created_at TEXT NOT NULL, source_run_id TEXT);
             INSERT INTO flow_suggestions VALUES ('s-ok', 't', 'o', 'r', NULL, '[]', '[]', '[]', 'b', 0.5, 'new', 'a', NULL);
             INSERT INTO flow_suggestions VALUES ('s-bad', 't', 'o', 'r', NULL, 'oops', '[]', '[]', 'b', 0.5, 'new', 'b', NULL);"
        ),
    );
    let catalog = target();
    let report = import_flow_catalog(dir.path(), &catalog).await.unwrap();

    assert_eq!(report.tally("flows").written, 1);
    assert_eq!(report.tally("flows").invalid, 1);
    assert_eq!(report.tally("revisions").written, 1);
    assert_eq!(report.tally("revisions").invalid, 2);
    assert_eq!(report.tally("runs").written, 1);
    assert_eq!(report.tally("runs").invalid, 3);
    assert_eq!(report.tally("state").written, 1);
    assert_eq!(report.tally("state").invalid, 1);
    assert_eq!(report.tally("suggestions").written, 1);
    assert_eq!(report.tally("suggestions").invalid, 1);
    assert!(!report.is_clean());
    assert!(report.problems().iter().any(|p| p.contains("flow 'broken'")));
    assert!(catalog.get_flow("good").await.unwrap().is_some());
    assert!(catalog.get_flow("broken").await.unwrap().is_none());
    assert!(catalog.get_flow_run("run-orphan").await.unwrap().is_none());
}

#[tokio::test]
async fn drafts_that_are_not_usable_are_counted_and_staging_files_are_ignored() {
    let dir = tempfile::tempdir().unwrap();
    let drafts = dir.path().join("drafts");
    std::fs::create_dir_all(&drafts).unwrap();
    let draft = |id: &str| {
        json!({ "id": id, "name": "d", "graph": {}, "origin": "chat",
                "created_at": "2025-01-01T00:00:00Z", "updated_at": "2025-01-01T00:00:00Z" })
            .to_string()
    };
    std::fs::write(drafts.join("fine.json"), draft("fine")).unwrap();
    std::fs::write(drafts.join("garbage.json"), "not json").unwrap();
    std::fs::write(drafts.join("unsafe.json"), draft("../escape")).unwrap();
    std::fs::write(drafts.join(".fine.json.tmp"), draft("tmp")).unwrap();
    std::fs::write(drafts.join(".staged.json"), draft("staged")).unwrap();
    std::fs::write(drafts.join("notes.txt"), "not a draft").unwrap();
    std::fs::create_dir_all(drafts.join("nested.json")).unwrap();

    let catalog = target();
    let report = import_flow_catalog(dir.path(), &catalog).await.unwrap();

    assert_eq!(report.tally("drafts").written, 1);
    assert_eq!(report.tally("drafts").invalid, 2);
    assert!(catalog.get_draft("fine").await.unwrap().is_some());
    assert_eq!(catalog.list_drafts().await.unwrap().len(), 1);
}

#[tokio::test]
async fn a_flows_db_that_is_not_a_database_is_an_error_not_a_silent_skip() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("flows.db"),
        b"this is not a sqlite database, only text that is long enough to have a header",
    )
    .unwrap();
    assert!(import_flow_catalog(dir.path(), &target()).await.is_err());
}

#[tokio::test]
async fn a_database_with_none_of_the_tables_imports_nothing() {
    let dir = tempfile::tempdir().unwrap();
    legacy_db(dir.path(), "CREATE TABLE unrelated (x INTEGER);");
    let report = import_flow_catalog(dir.path(), &target()).await.unwrap();
    assert_eq!(report.total_written(), 0);
}

#[tokio::test]
async fn an_unlistable_drafts_path_is_an_error() {
    let dir = tempfile::tempdir().unwrap();
    // `drafts` is a file, so it cannot be listed as a directory.
    std::fs::write(dir.path().join("drafts"), "x").unwrap();
    assert!(import_flow_catalog(dir.path(), &target()).await.is_err());
}
