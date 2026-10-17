use super::*;
use crate::catalog::test_support::{catalog, trigger_graph};
use tinyflows_catalog::{DraftOrigin, FlowRunStep, SuggestionStatus};

/// The JSON form, for the model types that are not `PartialEq`.
fn j<T: serde::Serialize>(value: &T) -> Value {
    serde_json::to_value(value).unwrap()
}

fn flow(id: &str) -> Flow {
    Flow {
        id: id.to_string(),
        name: format!("flow {id}"),
        graph: trigger_graph(),
        enabled: true,
        created_at: "2025-03-01T10:00:00.000+00:00".to_string(),
        updated_at: "2025-03-02T10:00:00.000+00:00".to_string(),
        last_run_at: Some("2025-03-02T11:00:00.000+00:00".to_string()),
        last_status: Some("completed".to_string()),
        require_approval: true,
    }
}

fn revision(id: &str, flow_id: &str) -> FlowRevision {
    FlowRevision {
        id: id.to_string(),
        flow_id: flow_id.to_string(),
        graph: serde_json::to_value(trigger_graph()).unwrap(),
        name: "older name".to_string(),
        require_approval: false,
        created_at: "2025-03-01T12:00:00.000+00:00".to_string(),
    }
}

#[tokio::test]
async fn imported_history_bootstraps_sequence_until_a_graph_update_commits_it() {
    let store = catalog();
    let imported = flow("legacy-sequence");
    store.import_flow(&imported).await.unwrap();
    assert_eq!(
        store.latest_revision_number(&imported.id).await.unwrap(),
        Some(1)
    );
    let docs = store.docs().await.unwrap();
    assert!(
        docs.get(DEFINITIONS, &imported.id)
            .await
            .unwrap()
            .unwrap()
            .doc
            .get("revision")
            .is_none()
    );

    let first = revision("legacy-revision-1", &imported.id);
    assert!(store.import_revision(&first).await.unwrap());
    assert_eq!(
        store.latest_revision_number(&imported.id).await.unwrap(),
        Some(2)
    );
    assert!(!store.import_revision(&first).await.unwrap());
    assert_eq!(
        store.latest_revision_number(&imported.id).await.unwrap(),
        Some(2)
    );

    assert!(
        store
            .import_revision(&revision("legacy-revision-2", &imported.id))
            .await
            .unwrap()
    );
    assert_eq!(
        store.latest_revision_number(&imported.id).await.unwrap(),
        Some(3)
    );
    let current = store.get_flow(&imported.id).await.unwrap().unwrap();
    store
        .update_flow_graph(
            &imported.id,
            "after import".into(),
            trigger_graph(),
            false,
            None,
            false,
            Some(&current.updated_at),
        )
        .await
        .unwrap();
    assert_eq!(
        store.latest_revision_number(&imported.id).await.unwrap(),
        Some(4)
    );
    assert_eq!(
        docs.get(DEFINITIONS, &imported.id)
            .await
            .unwrap()
            .unwrap()
            .doc["revision"],
        4
    );
    assert!(!store.import_flow(&imported).await.unwrap());
    assert!(
        store
            .import_revision(&revision("legacy-revision-3", &imported.id))
            .await
            .unwrap()
    );
    assert_eq!(
        store.latest_revision_number(&imported.id).await.unwrap(),
        Some(4),
        "adding historical data leaves the current graph revision alone"
    );
}

fn run(id: &str, flow_id: &str, status: &str) -> FlowRun {
    FlowRun {
        id: id.to_string(),
        flow_id: flow_id.to_string(),
        thread_id: format!("thread-{id}"),
        status: status.to_string(),
        started_at: "2025-03-02T11:00:00.000+00:00".to_string(),
        finished_at: Some("2025-03-02T11:00:05.000+00:00".to_string()),
        steps: vec![FlowRunStep {
            node_id: "t".to_string(),
            output: json!([{ "a": 1 }]),
            port: Some("main".to_string()),
            status: Some("success".to_string()),
            duration_ms: Some(7),
            diagnostics: vec![json!({ "location": "args.to", "expression": "=item.to" })],
        }],
        pending_approvals: vec!["approval-1".to_string()],
        error: Some("boom".to_string()),
        graph_hash: Some("abc123".to_string()),
    }
}

#[tokio::test]
async fn an_imported_flow_keeps_its_id_and_every_field() {
    let store = catalog();
    let original = flow("legacy-1");
    assert!(store.import_flow(&original).await.unwrap());
    let stored = store.get_flow("legacy-1").await.unwrap().unwrap();
    assert_eq!(j(&stored), j(&original));
}

#[tokio::test]
async fn importing_twice_keeps_the_first_copy() {
    let store = catalog();
    let original = flow("f");
    assert!(store.import_flow(&original).await.unwrap());
    let mut changed = flow("f");
    changed.name = "changed".to_string();
    assert!(!store.import_flow(&changed).await.unwrap());
    assert_eq!(
        store.get_flow("f").await.unwrap().unwrap().name,
        original.name
    );
}

#[tokio::test]
async fn an_import_never_replaces_a_flow_the_catalog_already_holds() {
    let store = catalog();
    let live = store
        .create_flow("live".into(), trigger_graph(), false, true)
        .await
        .unwrap();
    let mut legacy = flow(&live.id);
    legacy.name = "stale legacy name".to_string();
    assert!(!store.import_flow(&legacy).await.unwrap());
    assert_eq!(
        store.get_flow(&live.id).await.unwrap().unwrap().name,
        "live"
    );
}

#[tokio::test]
async fn imported_revisions_are_visible_newest_first() {
    let store = catalog();
    store.import_flow(&flow("f")).await.unwrap();
    let older = revision("r-old", "f");
    let mut newer = revision("r-new", "f");
    newer.created_at = "2025-03-01T13:00:00.000+00:00".to_string();
    assert!(store.import_revision(&older).await.unwrap());
    assert!(store.import_revision(&newer).await.unwrap());
    assert!(!store.import_revision(&older).await.unwrap());
    let listed = store.list_revisions("f", 10).await.unwrap();
    assert_eq!(
        listed.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
        ["r-new", "r-old"]
    );
    assert_eq!(listed[1], older);
    assert_eq!(
        store.revision_by_id("f", "r-old").await.unwrap(),
        Some(older)
    );
}

#[tokio::test]
async fn imported_revisions_and_runs_need_their_flow() {
    let store = catalog();
    let error = store
        .import_revision(&revision("r", "missing"))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("flow 'missing' does not exist"));
    let error = store
        .import_run(&run("x", "missing", "completed"))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("flow 'missing' does not exist"));
}

#[tokio::test]
async fn an_imported_run_keeps_steps_approvals_error_and_hash() {
    let store = catalog();
    store.import_flow(&flow("f")).await.unwrap();
    let original = run("run-1", "f", "completed");
    assert!(store.import_run(&original).await.unwrap());
    assert!(!store.import_run(&original).await.unwrap());
    let stored = store.get_flow_run("run-1").await.unwrap().unwrap();
    assert_eq!(j(&stored), j(&original));
    let listed = store.list_flow_runs("f", 10).await.unwrap();
    assert_eq!(listed.iter().map(j).collect::<Vec<_>>(), vec![j(&original)]);
}

#[tokio::test]
async fn an_imported_unfinished_run_stays_live_and_can_settle() {
    let store = catalog();
    store.import_flow(&flow("f")).await.unwrap();
    let mut live = run("run-live", "f", "running");
    live.finished_at = None;
    live.error = None;
    live.graph_hash = None;
    live.pending_approvals.clear();
    store.import_run(&live).await.unwrap();
    assert_eq!(
        store
            .get_flow_run("run-live")
            .await
            .unwrap()
            .unwrap()
            .status,
        "running"
    );
    assert!(
        store
            .finish_flow_run(
                "run-live",
                "completed",
                "2025-03-03T00:00:00.000+00:00",
                &[],
                &[],
                None,
                None
            )
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn imported_state_is_what_the_state_store_reads_and_is_not_overwritten() {
    let store = catalog();
    assert!(
        store
            .import_kv("workflow:a", "k", &json!({ "n": 1 }))
            .await
            .unwrap()
    );
    assert!(
        !store
            .import_kv("workflow:a", "k", &json!({ "n": 2 }))
            .await
            .unwrap()
    );
    assert_eq!(
        store.kv_get("workflow:a", "k").await.unwrap(),
        Some(json!({ "n": 1 }))
    );
}

#[tokio::test]
async fn an_imported_suggestion_keeps_its_status_and_creation_time() {
    let store = catalog();
    let suggestion = FlowSuggestion {
        id: "s-1".to_string(),
        title: "Digest".to_string(),
        one_liner: "Send a digest".to_string(),
        rationale: "You do it daily".to_string(),
        trigger_hint: Some("daily".to_string()),
        steps_outline: vec!["fetch".to_string(), "send".to_string()],
        suggested_connections: vec!["gmail".to_string()],
        suggested_slugs: vec!["GMAIL_SEND".to_string()],
        build_prompt: "build it".to_string(),
        confidence: 0.75,
        status: SuggestionStatus::Dismissed,
        created_at: "2025-03-01T10:00:00.000+00:00".to_string(),
        source_run_id: Some("run-1".to_string()),
    };
    assert!(store.import_suggestion(&suggestion).await.unwrap());
    assert!(!store.import_suggestion(&suggestion).await.unwrap());
    let listed = store.list_suggestions(None, 10).await.unwrap();
    assert_eq!(listed, vec![suggestion]);
}

#[tokio::test]
async fn an_imported_draft_keeps_its_id_and_timestamps() {
    let store = catalog();
    let draft = FlowDraft {
        id: "draft-1".to_string(),
        flow_id: Some("f".to_string()),
        name: "wip".to_string(),
        graph: json!({ "nodes": [] }),
        origin: DraftOrigin::Import,
        created_at: "2025-03-01T10:00:00.000+00:00".to_string(),
        updated_at: "2025-03-01T11:00:00.000+00:00".to_string(),
    };
    assert!(store.import_draft(&draft).await.unwrap());
    assert!(!store.import_draft(&draft).await.unwrap());
    assert_eq!(store.get_draft("draft-1").await.unwrap(), Some(draft));
}

#[tokio::test]
async fn a_draft_with_an_unsafe_id_is_refused() {
    let store = catalog();
    let draft = FlowDraft {
        id: "../escape".to_string(),
        flow_id: None,
        name: "x".to_string(),
        graph: json!({}),
        origin: DraftOrigin::Chat,
        created_at: "2025-03-01T10:00:00.000+00:00".to_string(),
        updated_at: "2025-03-01T10:00:00.000+00:00".to_string(),
    };
    assert!(store.import_draft(&draft).await.is_err());
}

#[tokio::test]
async fn removing_an_imported_flow_removes_its_imported_history() {
    let store = catalog();
    store.import_flow(&flow("f")).await.unwrap();
    store.import_revision(&revision("r", "f")).await.unwrap();
    store
        .import_run(&run("run-1", "f", "completed"))
        .await
        .unwrap();
    store.remove_flow("f").await.unwrap();
    assert!(store.list_revisions("f", 10).await.unwrap().is_empty());
    assert!(store.get_flow_run("run-1").await.unwrap().is_none());
}
