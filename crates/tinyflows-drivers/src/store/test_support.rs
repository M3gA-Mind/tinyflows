//! Fixtures the store tests share.

use std::sync::Arc;

use serde_json::json;
use tinyflows::store::types::WorkflowRecord;
use tinyflows::store::parse_workflow;
use tinystoragedrivers_core::{DocumentStore, MemoryStorage, Scope, StorageBackend};

use super::DriverWorkflowStore;

/// `storage`'s documents under `scope`.
pub(crate) fn docs_in(storage: &MemoryStorage, scope: &str) -> Arc<dyn DocumentStore> {
    Arc::clone(
        storage
            .for_scope(&Scope::new(scope).unwrap())
            .unwrap()
            .documents(),
    )
}

/// A store over a fresh in-memory backend.
pub(crate) fn store() -> DriverWorkflowStore {
    DriverWorkflowStore::new(docs_in(&MemoryStorage::new(), "local"))
}

/// A second store instance over the same storage as `first`, as a daemon task
/// or another process would open.
pub(crate) fn store_over(storage: &MemoryStorage, scope: &str) -> DriverWorkflowStore {
    DriverWorkflowStore::new(docs_in(storage, scope))
}

/// The smallest document that validates: one trigger, one transform, one edge.
pub(crate) fn valid_document(id: &str) -> String {
    json!({
        "id": id,
        "name": "Greet",
        "description": "says hello",
        "nodes": [
            { "id": "t", "kind": "trigger", "name": "start",
              "config": { "trigger_kind": "manual" } },
            { "id": "greet", "kind": "transform", "name": "greet",
              "config": { "set": { "greeting": "=.item.name" } } }
        ],
        "edges": [
            { "from_node": "t", "from_port": "main", "to_node": "greet", "to_port": "main" }
        ]
    })
    .to_string()
}

/// A valid record with `id` and `description`.
pub(crate) fn record(id: &str, description: &str) -> WorkflowRecord {
    let mut record = parse_workflow(&valid_document(id), id).expect("a valid document");
    record.description = description.to_string();
    record
}
