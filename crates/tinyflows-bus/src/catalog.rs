//! Wire contract for workflow definitions and their revision history.
//!
//! Storage remains host-owned. Each request carries the callback bound by the
//! host to one acting agent, and the module performs catalog operations over
//! that scoped document port.

mod storage_types;
mod types;
mod extended_types;

pub use storage_types::{
    CatalogCollectionSpec, CatalogDocumentCursor, CatalogDocumentPage, CatalogDocumentPrecondition,
    CatalogFilter, CatalogIndexSpec, CatalogQuery, CatalogSearchSpec, CatalogSort,
    CatalogSortDirection, CatalogStorageFailure, CatalogStorageReply, CatalogVersionedDocument,
    DocDeleteRequest, DocDeleteWhereRequest, DocEnsureRequest, DocGetRequest, DocPutRequest,
    DocQueryRequest,
};

pub use types::{
    CallbackAddress, CatalogStorage, DeleteWorkflowResult, FlowUpdateError, GetRevisionRequest,
    ListRevisionsRequest, ListWorkflowsResult, PutWorkflowRequest, PutWorkflowResponse,
    PutWorkflowResult, SetEnabledRequest, Workflow, WorkflowRevision, WorkflowSummary,
};
pub use extended_types::*;

/// Catalog interface on the shared module bus name.
pub const INTERFACE: &str = "ai.tinyhumans.tinyflows.Catalog";
/// Catalog object path.
pub const OBJECT_PATH: &str = "/ai/tinyhumans/tinyflows/Catalog";
/// Version of the served catalog contract. Version 3 adds drafts, suggestions,
/// run reads, and host-mediated draft promotion.
pub const CONTRACT_VERSION: u32 = 3;
/// Exact members served by the catalog module.
pub const METHODS: &[&str] = &[
    members::LIST_WORKFLOWS,
    members::GET_WORKFLOW,
    members::PUT_WORKFLOW,
    members::DELETE_WORKFLOW,
    members::SET_ENABLED,
    members::LIST_REVISIONS,
    members::GET_REVISION,
    members::CREATE_DRAFT,
    members::GET_DRAFT,
    members::UPDATE_DRAFT,
    members::LIST_DRAFTS,
    members::DELETE_DRAFT,
    members::PROMOTE_DRAFT,
    members::UPSERT_SUGGESTIONS,
    members::LIST_SUGGESTIONS,
    members::SET_SUGGESTION_STATUS,
    members::GET_RUN,
    members::LIST_RUNS,
    members::LIST_ALL_RUNS,
    members::PRUNE_RUNS,
];

/// Callback interface implemented by the scope-owning host.
pub const STORAGE_INTERFACE: &str = "ai.tinyhumans.tinyflows.CatalogStorage";
/// Scope-bound host callback used only for draft promotion saves.
pub const DRAFT_PROMOTION_HOST_INTERFACE: &str = "ai.tinyhumans.tinyflows.DraftPromotionHost";
/// Stable members of the draft-promotion host callback.
pub mod draft_promotion_host {
    /// `(SavePromotedDraftRequest) -> SavePromotedDraftResult`.
    pub const SAVE_PROMOTED_DRAFT: &str = "SavePromotedDraft";
    /// Methods accepted by the module when it calls the promotion host.
    pub const METHODS: &[&str] = &[SAVE_PROMOTED_DRAFT];
}

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
    /// `(CreateDraftRequest) -> FlowDraft`.
    pub const CREATE_DRAFT: &str = "CreateDraft";
    /// `(DraftIdRequest) -> Option<FlowDraft>`.
    pub const GET_DRAFT: &str = "GetDraft";
    /// `(UpdateDraftRequest) -> FlowDraft`.
    pub const UPDATE_DRAFT: &str = "UpdateDraft";
    /// `(CatalogStorage) -> FlowDraft[]`.
    pub const LIST_DRAFTS: &str = "ListDrafts";
    /// `(DraftIdRequest) -> bool`.
    pub const DELETE_DRAFT: &str = "DeleteDraft";
    /// `(PromoteDraftRequest) -> PromoteDraftResult`.
    pub const PROMOTE_DRAFT: &str = "PromoteDraft";
    /// `(UpsertSuggestionsRequest) -> usize`.
    pub const UPSERT_SUGGESTIONS: &str = "UpsertSuggestions";
    /// `(ListSuggestionsRequest) -> FlowSuggestion[]`.
    pub const LIST_SUGGESTIONS: &str = "ListSuggestions";
    /// `(SetSuggestionStatusRequest) -> bool`.
    pub const SET_SUGGESTION_STATUS: &str = "SetSuggestionStatus";
    /// `(RunIdRequest) -> Option<FlowRun>`.
    pub const GET_RUN: &str = "GetRun";
    /// `(ListRunsRequest) -> FlowRun[]`.
    pub const LIST_RUNS: &str = "ListRuns";
    /// `(ListAllRunsRequest) -> FlowRun[]`.
    pub const LIST_ALL_RUNS: &str = "ListAllRuns";
    /// `(PruneRunsRequest) -> usize`.
    pub const PRUNE_RUNS: &str = "PruneRuns";
}

/// Callback members accepted by the generic document adapter.
pub mod storage {
    /// `(DocGetRequest) -> {value: Option<CatalogVersionedDocument>}`.
    pub const GET: &str = "DocGet";
    /// `(DocPutRequest) -> {value: u64}`.
    pub const PUT: &str = "DocPut";
    /// `(DocDeleteRequest) -> {value: bool}`.
    pub const DELETE: &str = "DocDelete";
    /// `(DocQueryRequest) -> {value: CatalogDocumentPage}`.
    pub const QUERY: &str = "DocQuery";
    /// `(DocEnsureRequest) -> {value: null}`.
    pub const ENSURE: &str = "DocEnsure";
    /// `(DocDeleteWhereRequest) -> {value: u64}`.
    pub const DELETE_WHERE: &str = "DocDeleteWhere";
}

/// Exact storage callback member set used by the served catalog.
pub const STORAGE_METHODS: &[&str] = &[
    storage::ENSURE,
    storage::GET,
    storage::PUT,
    storage::DELETE,
    storage::QUERY,
    storage::DELETE_WHERE,
];

#[cfg(test)]
#[path = "catalog_tests.rs"]
mod tests;
