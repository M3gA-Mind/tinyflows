//! Wire contract for workflow definitions and their revision history.
//!
//! Storage remains host-owned. Each request carries the callback bound by the
//! host to one acting agent, and the module performs catalog operations over
//! that scoped document port.

mod storage_types;
mod types;

pub use storage_types::{
    CatalogStorageFailure, CatalogStorageReply, DocAtomicBatchRequest, DocCapabilities,
    DocCapabilitiesRequest, DocCountRequest, DocDeleteRequest, DocDeleteWhereRequest,
    DocEnsureRequest, DocGetRequest, DocPutRequest, DocQueryRequest,
};

pub use types::{
    CallbackAddress, CatalogStorage, DeleteWorkflowResult, FlowUpdateError, GetRevisionRequest,
    ListRevisionsRequest, ListWorkflowsResult, PutWorkflowRequest, PutWorkflowResponse,
    PutWorkflowResult, SetEnabledRequest, Workflow, WorkflowRevision, WorkflowSummary,
};

/// Catalog interface on the shared module bus name.
pub const INTERFACE: &str = "ai.tinyhumans.tinyflows.Catalog";
/// Catalog object path.
pub const OBJECT_PATH: &str = "/ai/tinyhumans/tinyflows/Catalog";
/// Version of the served catalog contract. Version 2 serves scoped document
/// storage, create/update/duplicate, enablement, and revision history.
pub const CONTRACT_VERSION: u32 = 2;
/// Exact members served by the catalog module.
pub const METHODS: &[&str] = &[
    members::LIST_WORKFLOWS,
    members::GET_WORKFLOW,
    members::PUT_WORKFLOW,
    members::DELETE_WORKFLOW,
    members::SET_ENABLED,
    members::LIST_REVISIONS,
    members::GET_REVISION,
];

/// Callback interface implemented by the scope-owning host.
pub const STORAGE_INTERFACE: &str = "ai.tinyhumans.tinyflows.CatalogStorage";

/// Stable member names, in [`METHODS`] order.
pub mod members {
    /// `(CatalogStorage) -> ListWorkflowsResult`.
    pub const LIST_WORKFLOWS: &str = "ListWorkflows";
    /// `(CatalogStorage, id) -> Option<Workflow>`.
    pub const GET_WORKFLOW: &str = "GetWorkflow";
    /// `(PutWorkflowRequest) -> PutWorkflowResponse`.
    pub const PUT_WORKFLOW: &str = "PutWorkflow";
    /// `(CatalogStorage, id) -> DeleteWorkflowResult`.
    pub const DELETE_WORKFLOW: &str = "DeleteWorkflow";
    /// `(SetEnabledRequest) -> Workflow`.
    pub const SET_ENABLED: &str = "SetEnabled";
    /// `(ListRevisionsRequest) -> WorkflowRevision[]`.
    pub const LIST_REVISIONS: &str = "ListRevisions";
    /// `(GetRevisionRequest) -> Option<WorkflowRevision>`.
    pub const GET_REVISION: &str = "GetRevision";
}

/// Callback members accepted by the generic document adapter.
pub mod storage {
    /// `(DocGetRequest) -> {value: driver Versioned<Value>}`.
    pub const GET: &str = "DocGet";
    /// `(DocPutRequest) -> {value: driver Version}`.
    pub const PUT: &str = "DocPut";
    /// `(DocDeleteRequest) -> {value: bool}`.
    pub const DELETE: &str = "DocDelete";
    /// `(DocQueryRequest) -> {value: driver Page<Versioned<Value>>}`.
    pub const QUERY: &str = "DocQuery";
    /// `(DocEnsureRequest) -> {value: null}`.
    pub const ENSURE: &str = "DocEnsure";
    /// `(DocCountRequest) -> {value: u64}`.
    pub const COUNT: &str = "DocCount";
    /// `(DocDeleteWhereRequest) -> {value: u64}`.
    pub const DELETE_WHERE: &str = "DocDeleteWhere";
    /// `(DocAtomicBatchRequest) -> {value: driver WriteResult[]}`.
    pub const ATOMIC_BATCH: &str = "DocAtomicBatch";
    /// `(DocCapabilitiesRequest) -> {value: DocCapabilities}`.
    pub const CAPABILITIES: &str = "DocCapabilities";
}

#[cfg(test)]
#[path = "catalog_tests.rs"]
mod tests;
