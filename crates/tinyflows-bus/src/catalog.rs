//! Skeleton vocabulary for workflow catalog CRUD and node-kind discovery.
//!
//! Not yet served by `tinyflows-module`; see `docs/tinyflows-bus-design.md`.
//! Persistence is the host's document port, never a module-owned database.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Catalog interface on the shared module bus name.
pub const INTERFACE: &str = "ai.tinyhumans.tinyflows.Catalog";
/// Served catalog object path.
pub const OBJECT_PATH: &str = "/ai/tinyhumans/tinyflows/Catalog";
/// Version of the catalog vocabulary.
pub const CONTRACT_VERSION: u32 = 1;
/// Catalog members.
pub const METHODS: &[&str] = &[
    "ListWorkflows",
    "GetWorkflow",
    "PutWorkflow",
    "DeleteWorkflow",
    "ListRuns",
    "NodeKinds",
    "ApplyGraphOps",
    "ApprovalManifest",
];

/// Host document-port callback interface used for catalog persistence.
pub const STORAGE_INTERFACE: &str = "ai.tinyhumans.tinyflows.CatalogStorage";

/// Document-port members (get/put/delete/query over scoped collections).
pub mod storage {
    /// Read one document.
    pub const GET: &str = "DocGet";
    /// Replace one document.
    pub const PUT: &str = "DocPut";
    /// Delete one document.
    pub const DELETE: &str = "DocDelete";
    /// Query a collection.
    pub const QUERY: &str = "DocQuery";
}

/// One node kind description for editors.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct NodeKindInfo {
    /// Snake-case kind id.
    pub kind: String,
    /// JSON schema for the node config.
    pub config_schema: Value,
    /// Output ports.
    pub ports: Vec<String>,
}

/// A catalog entry summary.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WorkflowSummary {
    /// Workflow id.
    pub id: String,
    /// Display name.
    pub name: String,
    /// Latest revision number.
    pub revision: u64,
}

/// A candidate node for the approval manifest, classified by the host.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ManifestCandidate {
    /// Node id.
    pub node_id: String,
    /// Tool slug or node kind that might prompt.
    pub subject: String,
}
