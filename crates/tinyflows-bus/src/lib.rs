//! Transport-free vocabulary for the loadable workflow authoring module.
//!
//! Graphs cross the boundary as JSON so hosts do not link the engine to read
//! authoring reports. Migration and interpretation belong to the module.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Module bus identity.
pub const BUS_NAME: &str = "ai.tinyhumans.tinyflows.Authoring";
/// Authoring interface name.
pub const INTERFACE: &str = "ai.tinyhumans.tinyflows.Authoring";
/// Served authoring object path.
pub const OBJECT_PATH: &str = "/ai/tinyhumans/tinyflows/Authoring";
/// Version of this wire contract.
pub const CONTRACT_VERSION: u32 = 1;
/// Authoring members provided by the module.
pub const METHODS: &[&str] = &[
    "ValidateGraph",
    "BindingFailures",
    "AuthoringFailures",
    "UnresolvableToolArgs",
];

/// Stable authoring member names.
pub mod methods {
    /// Migrate, parse, structurally validate and check engine compatibility.
    pub const VALIDATE_GRAPH: &str = "ValidateGraph";
    /// Check bindings that omit the required node-output envelope.
    pub const BINDING_FAILURES: &str = "BindingFailures";
    /// Check portable binding, agent prompt/schema, and code-language gates.
    pub const AUTHORING_FAILURES: &str = "AuthoringFailures";
    /// Mock-run outbound arguments, skipping host-provided native slug prefixes.
    pub const UNRESOLVABLE_TOOL_ARGS: &str = "UnresolvableToolArgs";
}

/// One structured graph validation failure.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ValidationError {
    /// Stable failure classification.
    pub code: String,
    /// Author-facing explanation supplied by the engine.
    pub message: String,
    /// Offending node, when known.
    pub node_id: Option<String>,
    /// Offending field, when known.
    pub field: Option<String>,
}

/// Result of migration and graph validation without persistence or host I/O.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct GraphValidation {
    /// Migrated graph when parsing succeeded, including invalid graphs so an
    /// editor can keep the author's work while presenting errors.
    pub graph: Option<Value>,
    /// Every failure from the first failing stage (parse, structure, topology).
    pub errors: Vec<ValidationError>,
}

impl GraphValidation {
    /// Whether all engine validation stages succeeded.
    #[must_use]
    pub fn is_valid(&self) -> bool {
        self.graph.is_some() && self.errors.is_empty()
    }
}
