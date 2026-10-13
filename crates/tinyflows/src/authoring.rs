//! The one door a raw graph value goes through before it is persisted.

use crate::model::WorkflowGraph;

/// Migrates a raw graph JSON value to the current schema, deserializes it,
/// rejects a structurally invalid graph ([`crate::validate::validate`]) and an
/// engine-incompatible topology ([`crate::compat::ensure_compatible`]). A bad
/// graph is caught here, before it is ever persisted. Pure: no persistence
/// effect.
///
/// # Errors
/// A human-readable message for the first failure.
pub fn validate_and_migrate_graph(graph_json: serde_json::Value) -> Result<WorkflowGraph, String> {
    let graph = crate::migrate::deserialize_graph(graph_json)?;
    crate::validate::validate(&graph).map_err(|e| e.to_string())?;
    crate::compat::ensure_compatible(&graph)?;
    Ok(graph)
}

#[cfg(test)]
#[path = "authoring_tests.rs"]
mod tests;
