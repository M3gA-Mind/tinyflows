//! Serde-only records for the served workflow catalog.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Address of a host callback object that is already bound to one agent scope.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct CallbackAddress {
    /// Host bus name.
    pub destination: String,
    /// Object path for this scope's callback implementation.
    pub object_path: String,
}

/// Catalog document port supplied by the scope-owning host.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct CatalogStorage {
    /// Callback whose authorization and storage scope are fixed by the host.
    pub callback: CallbackAddress,
}

/// A saved workflow crossing the module boundary.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct Workflow {
    /// Stable workflow id.
    pub id: String,
    /// Display name.
    pub name: String,
    /// Whether triggers may run this workflow.
    pub enabled: bool,
    /// Portable graph JSON.
    pub graph: Value,
    /// RFC3339 creation time.
    pub created_at: String,
    /// RFC3339 last update time and optimistic concurrency token.
    pub updated_at: String,
    /// RFC3339 time of the latest run, if any.
    pub last_run_at: Option<String>,
    /// Latest run status, if any.
    pub last_status: Option<String>,
    /// Whether outbound actions require approval.
    pub require_approval: bool,
}

/// A compact workflow row for list consumers.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WorkflowSummary {
    /// Stable workflow id.
    pub id: String,
    /// Display name.
    pub name: String,
    /// Current revision number (the initial saved definition is revision 1).
    pub revision: u64,
    /// Whether triggers may run this workflow.
    pub enabled: bool,
    /// Whether outbound actions require approval.
    pub require_approval: bool,
    /// Latest run status, if any.
    pub last_status: Option<String>,
    /// Trigger kind from the trigger node, if present.
    pub trigger_kind: Option<String>,
    /// Number of graph nodes.
    pub node_count: usize,
    /// RFC3339 last update time.
    pub updated_at: String,
}

/// A prior workflow revision, represented with raw graph JSON for forward compatibility.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WorkflowRevision {
    /// Stable revision id.
    pub id: String,
    /// Workflow id.
    pub flow_id: String,
    /// Graph snapshot.
    pub graph: Value,
    /// Workflow name at the snapshot.
    pub name: String,
    /// Approval setting at the snapshot.
    pub require_approval: bool,
    /// RFC3339 capture time.
    pub created_at: String,
}

/// Create, update, or duplicate a workflow through one stable member.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum PutWorkflowRequest {
    /// Create a new saved graph. Automatic triggers are born disabled.
    Create {
        /// Context-bound storage callback.
        storage: CatalogStorage,
        /// Display name.
        name: String,
        /// Candidate graph JSON.
        graph: Value,
        /// Requested approval requirement (the module may force it on).
        require_approval: bool,
    },
    /// Update an existing definition with optimistic concurrency.
    Update {
        /// Context-bound storage callback.
        storage: CatalogStorage,
        /// Workflow id.
        id: String,
        /// New display name.
        name: String,
        /// New graph JSON.
        graph: Value,
        /// Requested approval requirement (the module may force it on).
        require_approval: bool,
        /// `updated_at` value previously read by the caller, when supplied.
        expected_version: Option<String>,
        /// Force disarm when saving an automatic trigger.
        #[serde(default)]
        force_disarm_automatic: bool,
    },
    /// Copy an existing workflow into a fresh, disabled definition.
    Duplicate {
        /// Context-bound storage callback.
        storage: CatalogStorage,
        /// Source workflow id.
        id: String,
        /// Name for the duplicate.
        name: String,
    },
}

/// Result of a successful create/update/duplicate operation.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PutWorkflowResult {
    /// Persisted definition.
    pub workflow: Workflow,
    /// True when policy forced a newly created automatic-trigger flow off.
    pub born_disabled: bool,
    /// True when the module forced approval for outbound side effects.
    pub approval_forced: bool,
}

/// Catalog update failure, preserving the current record for a conflict.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "code", rename_all = "snake_case")]
pub enum FlowUpdateError {
    /// No workflow with the requested id exists.
    NotFound { message: String },
    /// Optimistic concurrency rejected the write; `current` is the fresh row.
    VersionConflict {
        /// Human-readable conflict explanation.
        message: String,
        /// Current workflow snapshot suitable for reload/diff.
        current: Workflow,
    },
    /// The document store could not complete the operation.
    Store { message: String },
}

/// Successful write result or a structured catalog failure.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "status", content = "value", rename_all = "snake_case")]
pub enum PutWorkflowResponse {
    /// Workflow was created, updated, or duplicated.
    Ok(PutWorkflowResult),
    /// The operation failed.
    Error(FlowUpdateError),
}

/// Workflow summaries plus rows skipped because their persisted graph is corrupt.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ListWorkflowsResult {
    /// Successfully decoded summaries.
    pub workflows: Vec<WorkflowSummary>,
    /// Corrupt or unmigratable rows omitted from the result.
    pub skipped_rows: usize,
}

/// Result of deleting a workflow.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct DeleteWorkflowResult {
    /// Id that was deleted.
    pub id: String,
    /// Always true for a successful delete response.
    pub removed: bool,
}

/// Enable or disable an existing workflow.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct SetEnabledRequest {
    /// Context-bound storage callback.
    pub storage: CatalogStorage,
    /// Workflow id.
    pub id: String,
    /// Desired trigger state.
    pub enabled: bool,
}

/// List prior snapshots for one workflow.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ListRevisionsRequest {
    /// Context-bound storage callback.
    pub storage: CatalogStorage,
    /// Workflow id.
    pub flow_id: String,
    /// Maximum number to return.
    pub limit: usize,
}

/// Read one prior snapshot.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct GetRevisionRequest {
    /// Context-bound storage callback.
    pub storage: CatalogStorage,
    /// Workflow id.
    pub flow_id: String,
    /// Revision id.
    pub revision_id: String,
}
