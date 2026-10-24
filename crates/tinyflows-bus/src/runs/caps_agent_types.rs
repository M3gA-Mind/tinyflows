//! Typed agent catalogue, run and cursor-poll records for `CapsHost`.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Host-visible advisory limits on an agent turn.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct AgentLimits {
    /// Maximum model/tool iterations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_steps: Option<u32>,
    /// Maximum tool calls.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tool_calls: Option<u32>,
    /// Whole-agent wall clock limit in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_timeout_secs: Option<u32>,
    /// Per-tool wall clock limit in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_timeout_secs: Option<u32>,
}

/// A tool grant attached to an agent definition.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct ToolGrant {
    /// Exact slug or trailing `.*` pattern.
    pub slug: String,
    /// Trusted host connection reference.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection_ref: Option<String>,
}

/// Context source kind in an agent definition.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ContextSourceKind {
    /// Literal or expression-resolved text.
    Text {
        #[serde(default)]
        text: Value,
    },
    /// Input items.
    Items,
    /// Memory recall through the existing memory capability.
    Memory {
        scope: String,
        query: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<u32>,
    },
    /// Host-defined flavour lookup.
    Flavour { slug: String },
    /// Host-defined context source and parameters.
    Host {
        source: String,
        #[serde(default, skip_serializing_if = "Value::is_null")]
        params: Value,
    },
}

/// One source declared in an agent definition.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct ContextSource {
    /// Optional editor label.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    /// Source declaration.
    #[serde(flatten)]
    pub kind: ContextSourceKind,
    /// Whether unresolved source absence is accepted.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub optional: bool,
}

/// Host-resolvable reusable agent definition.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct AgentDefinition {
    /// Stable agent id.
    pub id: String,
    /// Display name.
    #[serde(default)]
    pub name: String,
    /// Description.
    #[serde(default)]
    pub description: String,
    /// Standing instructions.
    #[serde(default)]
    pub instructions: Option<String>,
    /// Preferred model.
    #[serde(default)]
    pub model: Option<String>,
    /// Preferred provider.
    #[serde(default)]
    pub provider: Option<String>,
    /// Declared working directory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub working_dir: Option<String>,
    /// Advisory agent-loop limits.
    #[serde(default)]
    pub limits: AgentLimits,
    /// Tool grants.
    #[serde(default)]
    pub tools: Vec<ToolGrant>,
    /// Context sources, in declaration order.
    #[serde(default)]
    pub context: Vec<ContextSource>,
    /// Host-specific passthrough metadata.
    #[serde(default)]
    pub metadata: Map<String, Value>,
}

/// Model and provider preference for one agent turn.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct AgentModelSelection {
    /// Preferred model id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Preferred provider id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
}

/// Agent task and structured inputs.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct AgentInput {
    /// Resolved task text.
    #[serde(default)]
    pub text: String,
    /// Input items in order.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub data: Vec<Value>,
}

/// One resolved context block.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct ContextBlock {
    /// Authored label or generated context label.
    pub label: String,
    /// Source kind wire name.
    pub source_kind: String,
    /// Renderable prose when present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    /// Structured context payload.
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub data: Value,
}

/// A concrete allowed tool descriptor.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct ToolDescriptor {
    /// Tool slug.
    pub slug: String,
    /// Display name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Description.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Input JSON schema, if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_schema: Option<Value>,
    /// Trusted connection reference.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection_ref: Option<String>,
}

/// Agent run identity used for attribution and budgeting, not authorization.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct AgentRunIdentity {
    /// Enclosing workflow run id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    /// Workflow id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<String>,
    /// Graph node id.
    pub node_id: String,
    /// Nested workflow depth.
    #[serde(default)]
    pub depth: u32,
    /// Input item index.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_index: Option<usize>,
}

/// Fully resolved request sent to the host agent runner.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct AgentRunRequest {
    /// Opaque module-generated id used only to cancel this individual callback.
    #[serde(default)]
    pub client_request_id: String,
    /// Merged agent definition.
    pub agent: AgentDefinition,
    /// Model selection.
    #[serde(default)]
    pub model: AgentModelSelection,
    /// Task and input items.
    #[serde(default)]
    pub input: AgentInput,
    /// Resolved context blocks.
    #[serde(default)]
    pub context: Vec<ContextBlock>,
    /// Narrowed tool descriptors.
    #[serde(default)]
    pub tools: Vec<ToolDescriptor>,
    /// Trusted host connection reference.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection_ref: Option<String>,
    /// Effective requested working directory (still host-validated).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub working_dir: Option<String>,
    /// Informational run identity.
    #[serde(default)]
    pub identity: AgentRunIdentity,
    /// Merged host metadata.
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub metadata: Map<String, Value>,
    /// Output JSON schema.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_schema: Option<Value>,
    /// Resolved node configuration, preserved for legacy harnesses.
    #[serde(default)]
    pub config: Value,
}

/// Cancel one in-flight or not-yet-admitted host agent callback.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct AgentCancelRequest {
    /// The exact id supplied in the matching `AgentRun` request.
    pub client_request_id: String,
}

/// Opaque host-generated handle for an in-flight agent turn.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct AgentRunHandle {
    /// Handle scoped to the exact callback address that created it.
    pub handle: String,
}

/// Cursor request for agent progress.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct AgentEventsRequest {
    /// Opaque handle returned by `AgentRun`.
    pub handle: String,
    /// Last accepted sequence, or zero before the first read.
    pub cursor: u64,
}

/// One bounded progress record from the host's agent loop.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct AgentProgressEvent {
    /// Strictly increasing sequence number for this handle.
    pub seq: u64,
    /// Host timestamp in epoch milliseconds.
    pub at_ms: i64,
    /// Host-defined event kind.
    pub kind: String,
    /// Renderable summary text.
    pub text: String,
}

/// Cursor response. A terminal response carries exactly one typed outcome.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct AgentEventsResponse {
    /// Events with sequence greater than the supplied cursor.
    pub events: Vec<AgentProgressEvent>,
    /// Whether the agent turn has settled.
    pub done: bool,
    /// Present exactly when `done` is true.
    pub outcome: Option<AgentRunOutcome>,
}

/// Stop reason for a host-owned agent loop.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(tag = "stop", rename_all = "snake_case")]
pub enum StopReason {
    /// Completed normally.
    Finished,
    /// A host-defined budget limit stopped the loop.
    LimitStop { limit: String },
    /// Host paused the run; module run management must preserve its token.
    Paused {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        token: Option<String>,
        reason: String,
        #[serde(default, skip_serializing_if = "Value::is_null")]
        payload: Value,
    },
}

/// Agent usage totals.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct AgentUsage {
    /// Prompt tokens.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_tokens: Option<u64>,
    /// Output tokens.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_tokens: Option<u64>,
    /// Model/tool steps.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub steps: Option<u32>,
    /// Tool calls.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<u32>,
}

/// Typed host agent result, including its settled transcript.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct AgentRunOutcome {
    /// Why the host loop stopped.
    pub stop: StopReason,
    /// Prose response.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    /// Structured response.
    #[serde(default)]
    pub json: Value,
    /// Native host payload.
    #[serde(default)]
    pub raw: Value,
    /// Optional usage figures.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<AgentUsage>,
    /// Settled agent transcript.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub transcript: Vec<TranscriptEntry>,
}

/// One settled line of host agent activity.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TranscriptEntry {
    /// Timestamp in epoch milliseconds.
    pub at_ms: i64,
    /// Host-defined kind.
    pub kind: String,
    /// Text bounded by the host before it is sent.
    pub text: String,
}

/// Result of resolving an agent workdir against the agent's workspace.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "status", content = "path", rename_all = "snake_case")]
pub enum WorkdirCheck {
    /// Host does not own this filesystem; use engine-local validation.
    Unmanaged,
    /// Canonical path on the agent's filesystem.
    Resolved(String),
    /// Host refused the path with this reason.
    Refused(String),
}

/// Request for a host context source.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct ResolveContextRequest {
    /// Host source id.
    pub source: String,
    /// Resolved source parameters.
    pub params: Value,
    /// Informational identity of the agent node.
    pub identity: AgentRunIdentity,
}

/// Request to expand grants against the host tool catalogue.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct ResolveToolsRequest {
    /// Allowed tool grants.
    pub grants: Vec<ToolGrant>,
    /// Trusted connection reference.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection_ref: Option<String>,
}

/// Request to resolve a declared workdir against an agent workspace.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ResolveWorkdirRequest {
    /// Agent's workspace root.
    pub workspace: String,
    /// Untrusted declared path.
    pub declared: String,
}
