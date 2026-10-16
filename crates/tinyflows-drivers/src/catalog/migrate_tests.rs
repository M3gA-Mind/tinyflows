use super::*;
use crate::catalog::FlowCatalogDocuments;
use serde_json::json;
use tinystoragedrivers_core::{
    DocumentStoreExt, MemoryStorage, MigrateOptions, Precondition, Query, Registry, Scope,
    ScopedStorage, StorageBackend,
};

fn scoped() -> ScopedStorage {
    MemoryStorage::new().for_scope(&Scope::local()).unwrap()
}

async fn seed(storage: &ScopedStorage, collection: &str, id: &str, body: Value) {
    storage
        .documents()
        .put(collection, id, body, Precondition::None)
        .await
        .unwrap();
}

async fn migrate(storage: &ScopedStorage) {
    let registry = Registry::compose([crate::storage_schema()]).unwrap();
    registry
        .migrate(storage, &MigrateOptions::new())
        .await
        .unwrap();
}

fn graph() -> Value {
    json!({"schema_version": 1, "nodes": [], "edges": []})
}

#[test]
fn the_schema_composes_and_names_every_catalog_collection() {
    let schema = crate::storage_schema();
    assert_eq!(schema.owner.as_str(), "flows");
    let names: Vec<&str> = schema.items.iter().map(|item| item.name()).collect();
    for name in [
        "flows.definitions",
        "flows.revisions",
        "flows.runs",
        "flows.suggestions",
        "flows.drafts",
    ] {
        assert!(names.contains(&name), "{name}");
    }
    assert!(schema.items.iter().all(|i| i.scope() == ScopeClass::Tenant));
    Registry::compose([schema]).unwrap();
}

#[tokio::test]
async fn v1_catalog_records_migrate_to_bare_ms_bodies_and_read_back() {
    let storage = scoped();
    let at = "2026-03-04T05:06:07.250+00:00";
    seed(
        &storage,
        "flows_definitions",
        "f1",
        json!({"name": "n", "graph_json": graph().to_string(), "enabled": true,
               "created_ns": 1, "created_at": at, "updated_at": at,
               "require_approval": false, "last_run_at": at, "last_status": "ok",
               "incarnation": "inc"}),
    )
    .await;
    seed(
        &storage,
        "flows_revisions",
        "r1",
        json!({"flow_id": "f1", "graph_json": graph().to_string(), "name": "old",
               "require_approval": false, "created_at": at, "created_ns": 1,
               "touched_ns": 5_000_000_000_i64, "flow_incarnation": "inc"}),
    )
    .await;
    seed(
        &storage,
        "flows_runs",
        "run1",
        json!({"flow_id": "f1", "thread_id": "t", "status": "success",
               "started_at": at, "started_ns": 1, "finished_at": at, "finished_ns": 2,
               "steps_json": "[{\"node_id\":\"a\",\"output\":1}]",
               "pending_approvals_json": "[\"x\"]", "flow_incarnation": "inc"}),
    )
    .await;
    seed(
        &storage,
        "flows_suggestions",
        "s1",
        json!({"title": "t", "one_liner": "o", "rationale": "r", "steps_json": "[\"a\"]",
               "connections_json": "[]", "slugs_json": "[\"s\"]", "build_prompt": "b",
               "confidence": 0.5, "status": "new", "created_at": at, "created_ns": 1}),
    )
    .await;
    let draft = json!({"id": "d1", "flow_id": "f1", "name": "d", "graph": {"a": 1},
                       "origin": "chat", "created_at": at, "updated_at": at});
    seed(
        &storage,
        "flows_drafts",
        "d1",
        json!({"draft_json": draft.to_string(), "updated_ns": 1}),
    )
    .await;

    migrate(&storage).await;

    let docs = storage.documents();
    let def = docs.get("flows.definitions", "f1").await.unwrap().unwrap();
    assert_eq!(def.doc["graph"], graph(), "the graph string is parsed");
    assert_eq!(def.doc["created_ms"], 1_772_600_767_250_i64);
    assert_eq!(def.doc["last_run_ms"], 1_772_600_767_250_i64);
    assert_eq!(
        def.meta.created_ms, 1_772_600_767_250_i64,
        "history keeps its time"
    );
    for gone in [
        "created_ns",
        "created_at",
        "updated_at",
        "last_run_at",
        "graph_json",
    ] {
        assert!(def.doc.get(gone).is_none(), "{gone}");
    }
    let rev = docs.get("flows.revisions", "r1").await.unwrap().unwrap();
    assert_eq!(rev.doc["touched_ms"], 5000);
    let run = docs.get("flows.runs", "run1").await.unwrap().unwrap();
    assert_eq!(run.doc["steps"][0]["node_id"], "a");
    assert_eq!(run.doc["pending_approvals"], json!(["x"]));
    assert_eq!(run.doc["finished_ms"], 1_772_600_767_250_i64);
    let sug = docs.get("flows.suggestions", "s1").await.unwrap().unwrap();
    assert_eq!(sug.doc["suggested_slugs"], json!(["s"]));
    let d = docs.get("flows.drafts", "d1").await.unwrap().unwrap();
    assert_eq!(d.doc["graph"], json!({"a": 1}));
    assert_eq!(d.doc["origin"], "chat");
    assert_eq!(d.doc["updated_ms"], 1_772_600_767_250_i64);

    // The migrated records read through the catalog.
    let catalog = FlowCatalogDocuments::new(std::sync::Arc::clone(docs));
    let (flows, skipped) = catalog.list_flows().await.unwrap();
    assert_eq!((flows.len(), skipped), (1, 0));
    assert_eq!(flows[0].created_at, "2026-03-04T05:06:07.250+00:00");
    assert_eq!(
        catalog
            .get_flow_run("run1")
            .await
            .unwrap()
            .unwrap()
            .steps
            .len(),
        1
    );
    assert_eq!(catalog.list_revisions("f1", 10).await.unwrap().len(), 1);
    assert_eq!(catalog.list_drafts().await.unwrap().len(), 1);
    assert_eq!(catalog.list_suggestions(None, 10).await.unwrap().len(), 1);
}

#[tokio::test]
async fn a_corrupt_v1_graph_string_stays_a_string_and_migrating_twice_changes_nothing() {
    let storage = scoped();
    seed(
        &storage,
        "flows_definitions",
        "bad",
        json!({"name": "n", "graph_json": "{not json", "enabled": false,
               "created_at": "2026-01-01T00:00:00Z", "updated_at": "2026-01-01T00:00:00Z"}),
    )
    .await;
    migrate(&storage).await;
    migrate(&storage).await;
    let rows = storage
        .documents()
        .query_all("flows.definitions", &Query::all())
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].doc["graph"], "{not json");
    let catalog = FlowCatalogDocuments::new(std::sync::Arc::clone(storage.documents()));
    let (flows, skipped) = catalog.list_flows().await.unwrap();
    assert_eq!((flows.len(), skipped), (0, 1), "still skipped as corrupt");
}
