//! Module-side workflow catalog operations over a host-scoped document port.

use anyhow::Result;
use serde_json::Value;
use std::sync::Arc;
use tinybus::{Connection, Error};
use tinyflows_bus::catalog::{
    self, ApprovalManifestRequest, ApplyGraphOpsRequest, ApplyGraphOpsResult, ApplyGraphOpsError,
    CatalogStorage, ClassifyManifestCandidatesRequest, CreateDraftRequest, DeleteWorkflowResult, DraftIdRequest, FlowDraft,
    FlowRun, FlowSuggestion, FlowUpdateError, ListAllRunsRequest, ListRevisionsRequest,
    ListRunsRequest, ListSuggestionsRequest, ListWorkflowsResult, PromoteDraftRequest,
    PromoteDraftResponse, PruneRunsRequest, PutWorkflowRequest, PutWorkflowResponse,
    PutWorkflowResult, RunIdRequest, SetEnabledRequest, SetSuggestionStatusRequest,
    UpdateDraftRequest, UpsertSuggestionsRequest, Workflow, WorkflowRevision, WorkflowSummary,
};
use tinyflows_catalog::Flow as StoredFlow;
use tinyflows_drivers::catalog::FlowCatalogDocuments;
use tinystoragedrivers_core::DocumentStore;

struct CatalogService {
    connection: Connection,
}

struct UpdateWorkflow {
    storage: CatalogStorage,
    id: String,
    name: String,
    graph: Value,
    require_approval: bool,
    expected_version: Option<String>,
    force_disarm_automatic: bool,
}

fn failure(error: impl std::fmt::Display) -> Error {
    Error::failed(error.to_string())
}

fn workflow(flow: StoredFlow) -> Workflow {
    Workflow {
        id: flow.id,
        name: flow.name,
        enabled: flow.enabled,
        graph: serde_json::to_value(flow.graph).expect("WorkflowGraph is JSON serializable"),
        created_at: flow.created_at,
        updated_at: flow.updated_at,
        last_run_at: flow.last_run_at,
        last_status: flow.last_status,
        require_approval: flow.require_approval,
    }
}

fn revision(revision: tinyflows_catalog::FlowRevision) -> WorkflowRevision {
    WorkflowRevision {
        id: revision.id,
        flow_id: revision.flow_id,
        graph: revision.graph,
        name: revision.name,
        require_approval: revision.require_approval,
        created_at: revision.created_at,
    }
}

fn graph(raw: Value) -> Result<tinyflows::model::WorkflowGraph> {
    let graph = tinyflows::migrate::deserialize_graph(raw).map_err(anyhow::Error::msg)?;
    tinyflows::validate::validate(&graph).map_err(anyhow::Error::msg)?;
    Ok(graph)
}

impl CatalogService {
    async fn node_kinds(&self) -> Vec<tinyflows_bus::node_kinds::NodeKindContract> {
        tinyflows_bus::node_kinds::all_contracts()
    }

    async fn apply_graph_ops(&self, request: ApplyGraphOpsRequest) -> ApplyGraphOpsResult {
        let graph = match tinyflows::migrate::deserialize_graph(request.graph) {
            Ok(graph) => graph,
            Err(details) => {
                return ApplyGraphOpsResult {
                    graph: None,
                    error: Some(ApplyGraphOpsError {
                        index: 0,
                        op: "decode_graph".into(),
                        details,
                    }),
                };
            }
        };
        let mut ops = Vec::with_capacity(request.ops.len());
        for (index, raw) in request.ops.into_iter().enumerate() {
            match serde_json::from_value::<tinyflows::graph_ops::GraphOp>(raw) {
                Ok(op) => ops.push(op),
                Err(error) => {
                    return ApplyGraphOpsResult {
                        graph: None,
                        error: Some(ApplyGraphOpsError {
                            index,
                            op: "decode_operation".into(),
                            details: error.to_string(),
                        }),
                    };
                }
            }
        }
        match tinyflows::graph_ops::apply_ops(&graph, &ops) {
            Ok(graph) => ApplyGraphOpsResult {
                graph: serde_json::to_value(graph).ok(),
                error: None,
            },
            Err(error) => ApplyGraphOpsResult {
                graph: None,
                error: Some(ApplyGraphOpsError {
                    index: error.index,
                    op: error.op.into(),
                    details: error.kind.to_string(),
                }),
            },
        }
    }

    async fn approval_manifest(
        &self,
        request: ApprovalManifestRequest,
    ) -> tinybus::Result<Vec<Value>> {
        let graph = tinyflows::migrate::deserialize_graph(request.graph).map_err(Error::failed)?;
        let candidates = tinyflows_catalog::approval_manifest::manifest_candidates(
            &graph,
            &request.native_prefix,
        );
        let callback = self
            .connection
            .proxy(
                &request.host.destination,
                &request.host.object_path,
                catalog::APPROVAL_MANIFEST_HOST_INTERFACE,
            )?;
        let classes: catalog::ClassifyManifestCandidatesResponse = callback
            .call(
                catalog::CLASSIFY_MANIFEST_CANDIDATES,
                (ClassifyManifestCandidatesRequest {
                    candidates: candidates.clone(),
                },),
            )
            .await?;
        if classes.len() != candidates.len() {
            return Err(Error::failed(format!(
                "manifest classifier returned {} classifications for {} candidates",
                classes.len(), candidates.len()
            )));
        }
        for (index, (candidate, class)) in candidates.iter().zip(&classes).enumerate() {
            let requires_class = matches!(
                candidate,
                catalog::ManifestCandidate::Http { .. }
                    | catalog::ManifestCandidate::Code { .. }
                    | catalog::ManifestCandidate::NativeTool { .. }
                    | catalog::ManifestCandidate::ConnectorTool { .. }
            );
            if requires_class != class.is_some() {
                return Err(Error::failed(format!(
                    "manifest classifier returned malformed classification at candidate {index}"
                )));
            }
        }
        Ok(tinyflows_catalog::approval_manifest::assemble_manifest(
            &candidates,
            &classes,
        ))
    }

    async fn store(&self, storage: CatalogStorage) -> Result<FlowCatalogDocuments> {
        let docs =
            super::catalog_documents::Documents::new(self.connection.clone(), storage.callback)
                .await?;
        Ok(FlowCatalogDocuments::new(
            Arc::new(docs) as Arc<dyn DocumentStore>
        ))
    }

    async fn summary(store: &FlowCatalogDocuments, flow: StoredFlow) -> Result<WorkflowSummary> {
        let trigger_kind = serde_json::to_value(&flow.graph)?
            .get("nodes")
            .and_then(Value::as_array)
            .and_then(|nodes| {
                nodes
                    .iter()
                    .find(|node| node.get("kind").and_then(Value::as_str) == Some("trigger"))
            })
            .and_then(|node| node.get("config"))
            .and_then(|config| config.get("trigger_kind"))
            .and_then(Value::as_str)
            .map(str::to_string);
        let revision = store.latest_revision_number(&flow.id).await?.unwrap_or(1);
        Ok(WorkflowSummary {
            id: flow.id,
            name: flow.name,
            revision,
            enabled: flow.enabled,
            require_approval: flow.require_approval,
            last_status: flow.last_status,
            trigger_kind,
            node_count: flow.graph.nodes.len(),
            updated_at: flow.updated_at,
        })
    }

    async fn put(&self, request: PutWorkflowRequest) -> PutWorkflowResponse {
        let response = match request {
            PutWorkflowRequest::Create {
                storage,
                name,
                graph: raw,
                require_approval,
            } => self.create(storage, name, raw, require_approval).await,
            PutWorkflowRequest::Update {
                storage,
                id,
                name,
                graph: raw,
                require_approval,
                expected_version,
                force_disarm_automatic,
            } => {
                self.update(UpdateWorkflow {
                    storage,
                    id,
                    name,
                    graph: raw,
                    require_approval,
                    expected_version,
                    force_disarm_automatic,
                })
                .await
            }
            PutWorkflowRequest::Duplicate { storage, id, name } => {
                self.duplicate(storage, id, name).await
            }
        };
        match response {
            Ok(value) => PutWorkflowResponse::Ok(value),
            Err(error) => PutWorkflowResponse::Error(error),
        }
    }

    async fn create(
        &self,
        storage: CatalogStorage,
        name: String,
        raw: Value,
        requested_approval: bool,
    ) -> std::result::Result<PutWorkflowResult, FlowUpdateError> {
        let store = self.store(storage).await.map_err(store_error)?;
        let graph = graph(raw).map_err(|error| FlowUpdateError::Store {
            message: error.to_string(),
        })?;
        let automatic = tinyflows_catalog::graph_policy::trigger_is_automatic(&graph);
        let approval_forced = !requested_approval
            && tinyflows_catalog::graph_policy::graph_has_outbound_side_effect(&graph);
        let flow = store
            .create_flow(
                name,
                graph,
                requested_approval || approval_forced,
                !automatic,
            )
            .await
            .map_err(|error| FlowUpdateError::Store {
                message: error.to_string(),
            })?;
        Ok(PutWorkflowResult {
            workflow: workflow(flow),
            born_disabled: automatic,
            approval_forced,
        })
    }

    async fn update(
        &self,
        request: UpdateWorkflow,
    ) -> std::result::Result<PutWorkflowResult, FlowUpdateError> {
        let UpdateWorkflow {
            storage,
            id,
            name,
            graph: raw,
            require_approval: requested_approval,
            expected_version,
            force_disarm_automatic,
        } = request;
        let store = self.store(storage).await.map_err(store_error)?;
        let current = store
            .get_flow(&id)
            .await
            .map_err(store_error)?
            .ok_or_else(|| FlowUpdateError::NotFound {
                message: "flow not found".into(),
            })?;
        let graph = graph(raw).map_err(|error| FlowUpdateError::Store {
            message: error.to_string(),
        })?;
        let automatic = tinyflows_catalog::graph_policy::trigger_is_automatic(&graph);
        let approval_forced = !requested_approval
            && tinyflows_catalog::graph_policy::graph_has_outbound_side_effect(&graph);
        let flow = store
            .update_flow_graph(
                &id,
                name,
                graph,
                requested_approval || approval_forced,
                None,
                force_disarm_automatic,
                expected_version.as_deref(),
            )
            .await
            .map_err(map_update_error)?;
        Ok(PutWorkflowResult {
            born_disabled: current.enabled && !flow.enabled && automatic,
            workflow: workflow(flow),
            approval_forced,
        })
    }

    async fn duplicate(
        &self,
        storage: CatalogStorage,
        id: String,
        name: String,
    ) -> std::result::Result<PutWorkflowResult, FlowUpdateError> {
        let store = self.store(storage).await.map_err(store_error)?;
        let source = store
            .get_flow(&id)
            .await
            .map_err(store_error)?
            .ok_or_else(|| FlowUpdateError::NotFound {
                message: "flow not found".into(),
            })?;
        let flow = store
            .insert_duplicate_flow(&source, name)
            .await
            .map_err(store_error)?;
        Ok(PutWorkflowResult {
            workflow: workflow(flow),
            born_disabled: true,
            approval_forced: false,
        })
    }
}

fn store_error(error: impl std::fmt::Display) -> FlowUpdateError {
    FlowUpdateError::Store {
        message: error.to_string(),
    }
}

fn map_update_error(error: tinyflows_catalog::store::FlowUpdateError) -> FlowUpdateError {
    match error {
        tinyflows_catalog::store::FlowUpdateError::NotFound => {
            FlowUpdateError::NotFound { message: "flow not found".into() }
        }
        tinyflows_catalog::store::FlowUpdateError::Conflict(current) => {
            FlowUpdateError::VersionConflict {
                message: "This flow changed since you loaded it. Reload to see the latest version, then reapply your change.".into(),
                current: Box::new(workflow(*current)),
            }
        }
        tinyflows_catalog::store::FlowUpdateError::Store(error) => store_error(error),
    }
}

#[tinybus::interface(name = "ai.tinyhumans.tinyflows.Catalog")]
impl CatalogService {
    async fn list_workflows(
        &self,
        storage: CatalogStorage,
    ) -> tinybus::Result<ListWorkflowsResult> {
        let store = self.store(storage).await.map_err(failure)?;
        let (flows, skipped_rows) = store.list_flows().await.map_err(failure)?;
        let mut workflows = Vec::with_capacity(flows.len());
        for flow in flows {
            workflows.push(Self::summary(&store, flow).await.map_err(failure)?);
        }
        Ok(ListWorkflowsResult {
            workflows,
            skipped_rows,
        })
    }

    async fn get_workflow(
        &self,
        storage: CatalogStorage,
        id: String,
    ) -> tinybus::Result<Option<Workflow>> {
        let flow = self
            .store(storage)
            .await
            .map_err(failure)?
            .get_flow(&id)
            .await
            .map_err(failure)?;
        Ok(flow.map(workflow))
    }

    async fn put_workflow(
        &self,
        request: PutWorkflowRequest,
    ) -> tinybus::Result<PutWorkflowResponse> {
        Ok(self.put(request).await)
    }

    async fn delete_workflow(
        &self,
        storage: CatalogStorage,
        id: String,
    ) -> tinybus::Result<DeleteWorkflowResult> {
        self.store(storage)
            .await
            .map_err(failure)?
            .remove_flow(&id)
            .await
            .map_err(failure)?;
        Ok(DeleteWorkflowResult { id, removed: true })
    }

    async fn set_enabled(&self, request: SetEnabledRequest) -> tinybus::Result<Workflow> {
        let flow = self
            .store(request.storage)
            .await
            .map_err(failure)?
            .set_enabled(&request.id, request.enabled)
            .await
            .map_err(failure)?;
        Ok(workflow(flow))
    }

    async fn list_revisions(
        &self,
        request: ListRevisionsRequest,
    ) -> tinybus::Result<Vec<WorkflowRevision>> {
        self.store(request.storage)
            .await
            .map_err(failure)?
            .list_revisions(&request.flow_id, request.limit)
            .await
            .map(|items| items.into_iter().map(revision).collect())
            .map_err(failure)
    }

    async fn get_revision(
        &self,
        request: catalog::GetRevisionRequest,
    ) -> tinybus::Result<Option<WorkflowRevision>> {
        self.store(request.storage)
            .await
            .map_err(failure)?
            .revision_by_id(&request.flow_id, &request.revision_id)
            .await
            .map(|item| item.map(revision))
            .map_err(failure)
    }

    async fn create_draft(&self, request: CreateDraftRequest) -> tinybus::Result<FlowDraft> {
        super::catalog_extended_ops::create_draft(&self.connection, request)
            .await
            .map_err(failure)
    }

    async fn get_draft(&self, request: DraftIdRequest) -> tinybus::Result<Option<FlowDraft>> {
        super::catalog_extended_ops::get_draft(&self.connection, request)
            .await
            .map_err(failure)
    }

    async fn update_draft(&self, request: UpdateDraftRequest) -> tinybus::Result<FlowDraft> {
        super::catalog_extended_ops::update_draft(&self.connection, request)
            .await
            .map_err(failure)
    }

    async fn list_drafts(&self, storage: CatalogStorage) -> tinybus::Result<Vec<FlowDraft>> {
        super::catalog_extended_ops::list_drafts(&self.connection, storage)
            .await
            .map_err(failure)
    }

    async fn delete_draft(&self, request: DraftIdRequest) -> tinybus::Result<bool> {
        super::catalog_extended_ops::delete_draft(&self.connection, request)
            .await
            .map_err(failure)
    }

    async fn promote_draft(
        &self,
        request: PromoteDraftRequest,
    ) -> tinybus::Result<PromoteDraftResponse> {
        super::catalog_extended_ops::promote_draft(&self.connection, request)
            .await
            .map_err(failure)
    }

    async fn upsert_suggestions(
        &self,
        request: UpsertSuggestionsRequest,
    ) -> tinybus::Result<usize> {
        super::catalog_extended_ops::upsert_suggestions(&self.connection, request)
            .await
            .map_err(failure)
    }

    async fn list_suggestions(
        &self,
        request: ListSuggestionsRequest,
    ) -> tinybus::Result<Vec<FlowSuggestion>> {
        super::catalog_extended_ops::list_suggestions(&self.connection, request)
            .await
            .map_err(failure)
    }

    async fn set_suggestion_status(
        &self,
        request: SetSuggestionStatusRequest,
    ) -> tinybus::Result<bool> {
        super::catalog_extended_ops::set_suggestion_status(&self.connection, request)
            .await
            .map_err(failure)
    }

    async fn get_run(&self, request: RunIdRequest) -> tinybus::Result<Option<FlowRun>> {
        super::catalog_extended_ops::get_run(&self.connection, request)
            .await
            .map_err(failure)
    }

    async fn list_runs(&self, request: ListRunsRequest) -> tinybus::Result<Vec<FlowRun>> {
        super::catalog_extended_ops::list_runs(&self.connection, request)
            .await
            .map_err(failure)
    }

    async fn list_all_runs(&self, request: ListAllRunsRequest) -> tinybus::Result<Vec<FlowRun>> {
        super::catalog_extended_ops::list_all_runs(&self.connection, request)
            .await
            .map_err(failure)
    }

    async fn prune_runs(&self, request: PruneRunsRequest) -> tinybus::Result<usize> {
        super::catalog_extended_ops::prune_runs(&self.connection, request)
            .await
            .map_err(failure)
    }
}

pub(super) async fn setup(connection: &Connection) -> Result<()> {
    connection
        .serve_at(
            catalog::OBJECT_PATH.try_into()?,
            CatalogService {
                connection: connection.clone(),
            },
        )
        .await?;
    Ok(())
}

#[cfg(test)]
#[path = "catalog_service_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "catalog_extended_service_tests.rs"]
mod extended_tests;

#[cfg(test)]
#[path = "catalog_extended_operations_tests.rs"]
mod operations_tests;
