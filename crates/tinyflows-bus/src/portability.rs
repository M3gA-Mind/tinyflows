//! Minimal JSON wire contract for portable flow preparation helpers.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Interface that exposes engine-owned preparation behavior to hosts.
pub const INTERFACE: &str = "ai.tinyhumans.tinyflows.Portability";
/// Contract version for this interface.
pub const CONTRACT_VERSION: u32 = 4;
/// Methods implemented by the compiled module.
pub const METHODS: &[&str] = &[
    methods::PREPARE_HTTP_REQUEST,
    methods::INSPECT_HTTP_REQUEST,
    methods::APP_EVENT_BINDING,
    methods::PINNED_INPUTS,
    methods::DESCRIBE_TRIGGER,
    methods::PREPARE_LLM_COMPLETE,
    methods::FORMAT_LLM_COMPLETE,
    methods::PREPARE_AGENT_RUN,
    methods::FORMAT_AGENT_RUN,
];

/// Stable member names.
pub mod methods {
    /// Prepare a request with a host-resolved credential.
    pub const PREPARE_HTTP_REQUEST: &str = "PrepareHttpRequest";
    /// Inspect a request before the host resolves its credential.
    pub const INSPECT_HTTP_REQUEST: &str = "InspectHttpRequest";
    /// Match a graph's sole app-event trigger.
    pub const APP_EVENT_BINDING: &str = "AppEventBinding";
    /// Read literal inputs from a graph's sole trigger.
    pub const PINNED_INPUTS: &str = "PinnedInputs";
    /// Describe the module-owned trigger metadata.
    pub const DESCRIBE_TRIGGER: &str = "DescribeTrigger";
    /// Prepare a generic completion request for the host inference provider.
    pub const PREPARE_LLM_COMPLETE: &str = "PrepareLlmComplete";
    /// Apply module-owned structured-output behavior to a host completion.
    pub const FORMAT_LLM_COMPLETE: &str = "FormatLlmComplete";
    /// Prepare the prompt, model, and timeout for a host agent run.
    pub const PREPARE_AGENT_RUN: &str = "PrepareAgentRun";
    /// Shape a host agent reply into the node's stable output value.
    pub const FORMAT_AGENT_RUN: &str = "FormatAgentRun";
}

/// Credential material already resolved by the trusted host.
#[derive(Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct PrepareHttpRequest {
    /// Request object to prepare.
    pub request: Value,
    /// Credential template selected by host credential policy.
    pub credential: HttpCredentialTemplate,
}

/// Inspect an HTTP request before the host resolves its credential.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct InspectHttpRequest {
    /// Request object whose headers and body are never returned.
    pub request: Value,
    /// Opaque host-managed connection reference, if present.
    pub connection_ref: Option<String>,
}

/// Credential lookup details and a method/URL summary for host approval.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct HttpRequestDetails {
    /// Exact name parsed from a recognized `http_cred:<name>` reference;
    /// no additional suffix normalization is applied.
    pub credential_name: Option<String>,
    /// Method and URL only; request headers and body are omitted, though the
    /// URL copied from the request may itself contain sensitive data.
    pub redacted_summary: String,
}

/// One normalized chat message for a host inference provider.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct CompletionMessage {
    /// Normalized role: `system`, `user`, `assistant`, or `tool`.
    pub role: String,
    /// Message text.
    pub content: String,
}

/// Prepare a TinyFlows completion request without exposing engine helpers to
/// the host.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct PrepareLlmComplete {
    /// Original module-owned request.
    pub request: Value,
}

/// Host-neutral completion data produced by the module.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PreparedLlmComplete {
    /// Messages in the order the model must receive them.
    pub messages: Vec<CompletionMessage>,
    /// Whether the module should extract structured JSON from the reply.
    pub structured_output: bool,
    /// Node-pinned model, before host-specific provider routing.
    pub node_model: Option<String>,
}

/// Format a host completion through the module's output contract.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct FormatLlmComplete {
    /// Original request used to determine structured-output behavior.
    pub request: Value,
    /// Host completion response, including its text and usage fields.
    pub response: Value,
}

/// Prepare a generic node request for a host-owned agent turn.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct PrepareAgentRun {
    /// Raw agent node config.
    pub request: Value,
    /// Custom host registry model fallback, if any.
    pub entry_model: Option<String>,
    /// Custom host registry system prompt, if any.
    pub system_prompt: Option<String>,
    /// Effective limit read from the built host agent.
    pub effective_iteration_cap: u32,
}

/// Module-prepared agent prompt and execution policy inputs.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PreparedAgentRun {
    /// Model pin selected from the node and host registry entry.
    pub model: Option<String>,
    /// Complete prompt, including module-owned context and output steering.
    pub prompt: String,
    /// Whether the final text should be parsed as structured JSON.
    pub structured_output: bool,
    /// Clamped requested timeout before iteration-cap adjustment.
    pub base_timeout_secs: u64,
    /// Effective host timeout after applying the iteration-cap rule.
    pub timeout_secs: u64,
}

/// Format a host agent's final text through the module's output contract.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct FormatAgentRun {
    /// Agent identifier used in the stable result envelope.
    pub agent_ref: String,
    /// Original agent node config.
    pub request: Value,
    /// Final host-generated text.
    pub final_text: String,
}

/// Portable HTTP credential forms. Secret fields are transport data and must
/// never be emitted in logs, errors, or recorded workflow output.
#[derive(Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "scheme", rename_all = "snake_case")]
pub enum HttpCredentialTemplate {
    /// Bearer authorization credential.
    Bearer { secret: String },
    /// Basic authorization credential.
    Basic { username: String, secret: String },
    /// Secret sent through a custom header.
    Header { header: String, secret: String },
    /// Value already rendered by the trusted host.
    Resolved { header: String, value: String },
}

/// Match a graph's sole app-event trigger against a toolkit and slug.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct AppEventBinding {
    /// Serialized workflow graph.
    pub graph: Value,
    /// Event toolkit identifier.
    pub toolkit: String,
    /// Event trigger slug.
    pub trigger_slug: String,
}

/// Read literal inputs pinned on a graph's sole trigger.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct PinnedInputs {
    /// Serialized workflow graph.
    pub graph: Value,
}

/// Describe the module-owned trigger metadata needed by host bindings.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct DescribeTrigger {
    /// Serialized workflow graph.
    pub graph: Value,
}

/// Normalized trigger details for a graph with exactly one recognized trigger.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct TriggerDetails {
    /// Stable snake-case trigger kind; absent when the graph has no unique,
    /// recognized trigger.
    pub kind: Option<String>,
    /// Typed schedule for a schedule trigger. Other trigger kinds have no
    /// schedule. A missing or malformed schedule is an operation error.
    pub schedule: Option<crate::schedule::Schedule>,
}

#[cfg(test)]
#[path = "portability_tests.rs"]
mod tests;
