//! Minimal JSON wire contract for portable flow preparation helpers.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Interface that exposes engine-owned preparation behavior to hosts.
pub const INTERFACE: &str = "ai.tinyhumans.tinyflows.Portability";
/// Contract version for this interface.
pub const CONTRACT_VERSION: u32 = 3;
/// Methods implemented by the compiled module.
pub const METHODS: &[&str] = &[
    "PrepareHttpRequest",
    "InspectHttpRequest",
    "AppEventBinding",
    "PinnedInputs",
    "DescribeTrigger",
];

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

/// Safe request details for host credential lookup and approval.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct HttpRequestDetails {
    /// Name from a recognized `http_cred:<name>` reference.
    pub credential_name: Option<String>,
    /// Method and URL only; request headers and body are omitted.
    pub redacted_summary: String,
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
