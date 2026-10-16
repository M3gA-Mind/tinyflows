//! Serde-only DTOs for catalog drafts, suggestions, run reads, and promotion.

use super::{CallbackAddress, CatalogStorage, Workflow};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

/// Draft source, mirrored so hosts need only the bus crate.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DraftOrigin {
    /// Draft authored in chat.
    Chat,
    /// Draft authored in the canvas.
    Canvas,
    /// Draft created by an import.
    Import,
}

/// An editable graph that may be incomplete or invalid.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct FlowDraft {
    /// Stable draft identifier.
    pub id: String,
    /// Existing workflow being edited, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flow_id: Option<String>,
    /// Display name to use when promoted.
    pub name: String,
    /// Raw graph JSON, including incomplete graphs.
    pub graph: Value,
    /// Where the draft originated.
    pub origin: DraftOrigin,
    /// RFC3339 creation time.
    pub created_at: String,
    /// RFC3339 update time.
    pub updated_at: String,
}

/// Create a new draft.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct CreateDraftRequest {
    /// Acting-scope document callback.
    pub storage: CatalogStorage,
    /// Workflow being edited, if any.
    pub flow_id: Option<String>,
    /// Draft display name.
    pub name: String,
    /// Raw graph JSON.
    pub graph: Value,
    /// Draft source.
    pub origin: DraftOrigin,
}

/// Address a draft by id.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct DraftIdRequest {
    /// Acting-scope document callback.
    pub storage: CatalogStorage,
    /// Draft id.
    pub id: String,
}

/// Patch a draft; `flow_id` preserves missing, explicit null, and string.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct UpdateDraftRequest {
    /// Acting-scope document callback.
    pub storage: CatalogStorage,
    /// Draft id.
    pub id: String,
    /// Optional replacement name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Optional replacement graph; explicit JSON `null` replaces it with raw null.
    #[serde(
        default,
        deserialize_with = "deserialize_graph_patch",
        skip_serializing_if = "Option::is_none"
    )]
    pub graph: Option<Value>,
    /// Missing means unchanged, null clears, and a string assigns.
    #[serde(
        default,
        deserialize_with = "deserialize_flow_id_patch",
        skip_serializing_if = "Option::is_none"
    )]
    pub flow_id: Option<Option<String>>,
}

fn deserialize_flow_id_patch<'de, D>(deserializer: D) -> Result<Option<Option<String>>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer).map(Some)
}

fn deserialize_graph_patch<'de, D>(deserializer: D) -> Result<Option<Value>, D::Error>
where
    D: Deserializer<'de>,
{
    Value::deserialize(deserializer).map(Some)
}

/// Draft promotion asks a host callback to execute its normal save path.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct PromoteDraftRequest {
    /// Acting-scope document callback used to load and remove the draft.
    pub storage: CatalogStorage,
    /// Scope-bound host save callback; no scope is accepted in its payload.
    pub host: CallbackAddress,
    /// Draft id.
    pub id: String,
    /// Approval preference, passed through to the normal save path.
    pub require_approval: Option<bool>,
}

/// Exact create-or-update choice made by the module from the loaded draft.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum DraftPromotionOperation {
    /// Save a new workflow.
    Create {
        /// Draft name.
        name: String,
        /// Draft graph.
        graph: Value,
        /// Effective approval preference.
        require_approval: bool,
    },
    /// Save changes to the draft's existing workflow.
    Update {
        /// Existing workflow id.
        id: String,
        /// Draft name.
        name: String,
        /// Draft graph.
        graph: Value,
        /// Approval preference, if supplied.
        require_approval: Option<bool>,
        /// Promotion intentionally uses no optimistic version constraint.
        expected_version: Option<String>,
    },
}

/// Save request sent only over the context-bound promotion callback.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SavePromotedDraftRequest {
    /// Operation selected from the draft's `flow_id`.
    pub operation: DraftPromotionOperation,
}

/// Result of the host's normal workflow save path.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SavePromotedDraftResult {
    /// Saved workflow.
    pub workflow: Workflow,
    /// Host-owned outcome data and logs, passed through without interpretation.
    pub metadata: Value,
}

/// Host save failure carried without collapsing its structured RPC details.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct DraftPromotionFailure {
    /// Stable host error code.
    pub code: String,
    /// Human-readable explanation.
    pub message: String,
    /// Host-owned structured error data.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

/// Typed host callback outcome.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "status", content = "value", rename_all = "snake_case")]
pub enum SavePromotedDraftResponse {
    /// Normal save succeeded.
    Ok(SavePromotedDraftResult),
    /// Normal save failed and the draft must be retained.
    Error(DraftPromotionFailure),
}

/// Save result plus the draft cleanup outcome.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct PromoteDraftResult {
    /// Saved workflow.
    pub workflow: Workflow,
    /// Opaque data returned by the host's normal save path.
    pub metadata: Value,
    /// Whether the draft was deleted after saving.
    pub draft_deleted: bool,
    /// Cleanup failure detail, when the save succeeded but deletion failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cleanup_warning: Option<String>,
}

/// Promotion success or the structured failure returned by the host save.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "status", content = "value", rename_all = "snake_case")]
pub enum PromoteDraftResponse {
    /// Workflow saved; draft cleanup was attempted.
    Ok(PromoteDraftResult),
    /// Host save failed; the draft remains available.
    Error(DraftPromotionFailure),
}

/// Workflow suggestion lifecycle status.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SuggestionStatus {
    /// Newly discovered.
    #[default]
    New,
    /// Dismissed by the user.
    Dismissed,
    /// Built into a workflow.
    Built,
}

/// A suggested automation, not a validated workflow graph.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct FlowSuggestion {
    /// Stable content hash.
    pub id: String,
    /// Short title.
    pub title: String,
    /// One-sentence description.
    pub one_liner: String,
    /// Why it may be useful.
    pub rationale: String,
    /// Suggested trigger kind.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trigger_hint: Option<String>,
    /// Plain-language steps.
    #[serde(default)]
    pub steps_outline: Vec<String>,
    /// Grounded connected-account references.
    #[serde(default)]
    pub suggested_connections: Vec<String>,
    /// Grounded action slugs.
    #[serde(default)]
    pub suggested_slugs: Vec<String>,
    /// Builder prompt.
    pub build_prompt: String,
    /// Confidence score.
    #[serde(default)]
    pub confidence: f64,
    /// Lifecycle status.
    #[serde(default)]
    pub status: SuggestionStatus,
    /// RFC3339 creation time.
    pub created_at: String,
    /// Source run, if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_run_id: Option<String>,
}

/// Batch suggestions to upsert.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct UpsertSuggestionsRequest {
    /// Acting-scope document callback.
    pub storage: CatalogStorage,
    /// Suggestions to write.
    pub suggestions: Vec<FlowSuggestion>,
}

/// Query suggestions by status with the driver's minimum limit semantics.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct ListSuggestionsRequest {
    /// Acting-scope document callback.
    pub storage: CatalogStorage,
    /// Optional lifecycle filter.
    pub status: Option<SuggestionStatus>,
    /// Requested maximum result count.
    pub limit: usize,
}

/// Set one suggestion's status.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct SetSuggestionStatusRequest {
    /// Acting-scope document callback.
    pub storage: CatalogStorage,
    /// Suggestion id.
    pub id: String,
    /// New status.
    pub status: SuggestionStatus,
}

/// One run-history step.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct FlowRunStep {
    /// Graph node id.
    pub node_id: String,
    /// Node output.
    pub output: Value,
    /// Chosen output port.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<String>,
    /// Step outcome.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    /// Executor duration in milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    /// Data binding diagnostics.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<Value>,
}

/// Persisted run history row.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct FlowRun {
    /// Stable run id.
    pub id: String,
    /// Workflow id.
    pub flow_id: String,
    /// Checkpointer thread id.
    pub thread_id: String,
    /// Forward-compatible status string.
    pub status: String,
    /// RFC3339 start time.
    pub started_at: String,
    /// RFC3339 finish time.
    pub finished_at: Option<String>,
    /// Per-node steps.
    #[serde(default)]
    pub steps: Vec<FlowRunStep>,
    /// Nodes awaiting approval.
    #[serde(default)]
    pub pending_approvals: Vec<String>,
    /// Failure detail.
    #[serde(default)]
    pub error: Option<String>,
    /// Graph hash used for approval freshness checks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graph_hash: Option<String>,
}

/// Identify one run.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct RunIdRequest {
    /// Acting-scope document callback.
    pub storage: CatalogStorage,
    /// Run id.
    pub id: String,
}

/// List recent runs for one workflow.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct ListRunsRequest {
    /// Acting-scope document callback.
    pub storage: CatalogStorage,
    /// Workflow id.
    pub flow_id: String,
    /// Requested maximum count; driver clamps to at least one.
    pub limit: usize,
}

/// List recent runs across the acting scope.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct ListAllRunsRequest {
    /// Acting-scope document callback.
    pub storage: CatalogStorage,
    /// Requested maximum count; driver clamps to at least one.
    pub limit: usize,
}

/// Prune terminal runs for one workflow while preserving live runs.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct PruneRunsRequest {
    /// Acting-scope document callback.
    pub storage: CatalogStorage,
    /// Workflow id.
    pub flow_id: String,
    /// Desired retention count; driver clamps to at least one.
    pub keep: usize,
}
