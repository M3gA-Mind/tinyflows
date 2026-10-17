//! Minimal JSON wire contract for portable flow preparation helpers.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Interface that exposes engine-owned preparation behavior to hosts.
pub const INTERFACE: &str = "ai.tinyhumans.tinyflows.Portability";
/// Contract version for this interface.
pub const CONTRACT_VERSION: u32 = 1;
/// Methods implemented by the compiled module.
pub const METHODS: &[&str] = &["PrepareHttpRequest", "AppEventBinding", "PinnedInputs"];

/// Credential material already resolved by the trusted host.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PrepareHttpRequest {
    /// Request object to prepare.
    pub request: Value,
    /// Header selected by host credential policy.
    pub header: String,
    /// Opaque header value; consumers must never log this field.
    pub value: String,
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

#[cfg(test)]
#[path = "portability_tests.rs"]
mod tests;
