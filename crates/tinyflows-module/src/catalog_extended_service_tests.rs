use super::tests::{
    connections, graph, put_proxy, scope, scope_with_documents, scope_with_draft_delete_failure,
};
use serde_json::json;
use std::sync::Arc;
use tinybus::Connection;
use tinyflows_bus::catalog::{
    self as catalog, CallbackAddress, CatalogStorage, CreateDraftRequest, DraftIdRequest,
    DraftOrigin, DraftPromotionFailure, DraftPromotionOperation, FlowDraft, FlowRun,
    FlowSuggestion, ListAllRunsRequest, ListRunsRequest, ListSuggestionsRequest,
    PromoteDraftRequest, PromoteDraftResponse, PruneRunsRequest, PutWorkflowRequest,
    PutWorkflowResponse, RunIdRequest, SavePromotedDraftRequest, SavePromotedDraftResponse,
    SavePromotedDraftResult, SetSuggestionStatusRequest, SuggestionStatus, UpdateDraftRequest,
    UpsertSuggestionsRequest,
};
use tinyflows_drivers::catalog::FlowCatalogDocuments;
use tinystoragedrivers_core::DocumentStore;

struct PromotionHost {
    connection: Connection,
    storage: CatalogStorage,
    fail: bool,
}

#[tinybus::interface(name = "ai.tinyhumans.tinyflows.DraftPromotionHost")]
impl PromotionHost {
    async fn save_promoted_draft(
        &self,
        request: SavePromotedDraftRequest,
    ) -> tinybus::Result<SavePromotedDraftResponse> {
        if self.fail {
            return Ok(SavePromotedDraftResponse::Error(DraftPromotionFailure {
                code: "invalid_graph".into(),
                message: "host validation rejected the graph".into(),
                details: Some(json!({"field":"nodes[0]"})),
            }));
        }
        let proxy = put_proxy(&self.connection).unwrap();
        let put = match request.operation {
            DraftPromotionOperation::Create {
                name,
                graph,
                require_approval,
            } => PutWorkflowRequest::Create {
                storage: self.storage.clone(),
                name,
                graph,
                require_approval,
            },
            DraftPromotionOperation::Update {
                id,
                name,
                graph,
                require_approval,
                expected_version,
            } => {
                assert_eq!(expected_version, None);
                let current: Option<catalog::Workflow> = proxy
                    .call("GetWorkflow", &(self.storage.clone(), id.clone()))
                    .await
                    .unwrap();
                let current = current.expect("updated flow exists");
                PutWorkflowRequest::Update {
                    storage: self.storage.clone(),
                    id,
                    name,
                    graph,
                    require_approval: require_approval.unwrap_or(current.require_approval),
                    expected_version: None,
                    force_disarm_automatic: false,
                }
            }
        };
        let result: PutWorkflowResponse = proxy.call("PutWorkflow", &(put,)).await.unwrap();
        match result {
            PutWorkflowResponse::Ok(result) => {
                Ok(SavePromotedDraftResponse::Ok(SavePromotedDraftResult {
                    workflow: result.workflow,
                    metadata: json!({"born_disabled": result.born_disabled, "approval_forced": result.approval_forced, "host_log": "saved"}),
                }))
            }
            PutWorkflowResponse::Error(error) => {
                Ok(SavePromotedDraftResponse::Error(DraftPromotionFailure {
                    code: "catalog_save_failed".into(),
                    message: format!("{error:?}"),
                    details: serde_json::to_value(error).ok(),
                }))
            }
        }
    }
}

async fn promotion_host(
    host: &Connection,
    storage: CatalogStorage,
    name: &str,
    fail: bool,
) -> CallbackAddress {
    let path = format!("/test/promotion/{name}");
    host.serve_at(
        path.parse().unwrap(),
        PromotionHost {
            connection: host.clone(),
            storage,
            fail,
        },
    )
    .await
    .unwrap();
    CallbackAddress {
        destination: "test.Promotion".into(),
        object_path: path,
    }
}

fn proxy(host: &Connection) -> tinybus::Proxy {
    put_proxy(host).unwrap()
}

async fn create_draft(
    proxy: &tinybus::Proxy,
    storage: CatalogStorage,
    flow_id: Option<String>,
) -> FlowDraft {
    proxy
        .call(
            "CreateDraft",
            &(CreateDraftRequest {
                storage,
                flow_id,
                name: "Draft".into(),
                graph: json!({"incomplete":true}),
                origin: DraftOrigin::Canvas,
            }),
        )
        .await
        .unwrap()
}

fn suggestion(id: &str) -> FlowSuggestion {
    FlowSuggestion {
        id: id.into(),
        title: "Suggestion".into(),
        one_liner: "One line".into(),
        rationale: "Useful".into(),
        trigger_hint: Some("manual".into()),
        steps_outline: vec!["step".into()],
        suggested_connections: vec!["http_cred:test".into()],
        suggested_slugs: vec!["provider:send".into()],
        build_prompt: "Build it".into(),
        confidence: 0.8,
        status: SuggestionStatus::New,
        created_at: "2026-01-01T00:00:00Z".into(),
        source_run_id: Some("source-run".into()),
    }
}

#[tokio::test]
async fn catalog_v3_operations_delegate_to_drivers_and_keep_agent_scopes() {
    let (_server, host) = connections().await;
    let (alice, alice_docs) = scope_with_documents(&host, "extended_alice").await;
    let bob = scope(&host, "extended_bob").await;
    let proxy = proxy(&host);

    let draft = create_draft(&proxy, alice.clone(), Some("old-flow".into())).await;
    let other_draft = create_draft(&proxy, alice.clone(), None).await;
    let bob_drafts: Vec<FlowDraft> = proxy.call("ListDrafts", &(bob.clone(),)).await.unwrap();
    assert!(bob_drafts.is_empty());
    let fetched: Option<FlowDraft> = proxy
        .call(
            "GetDraft",
            &(DraftIdRequest {
                storage: alice.clone(),
                id: draft.id.clone(),
            },),
        )
        .await
        .unwrap();
    assert_eq!(fetched, Some(draft.clone()));
    let updated: FlowDraft = proxy
        .call(
            "UpdateDraft",
            &(UpdateDraftRequest {
                storage: alice.clone(),
                id: draft.id.clone(),
                name: Some("Edited".into()),
                graph: None,
                flow_id: Some(None),
            },),
        )
        .await
        .unwrap();
    assert_eq!(updated.name, "Edited");
    assert_eq!(updated.flow_id, None);
    let listed: Vec<FlowDraft> = proxy.call("ListDrafts", &(alice.clone(),)).await.unwrap();
    assert_eq!(listed.len(), 2);
    assert_eq!(
        proxy
            .call::<bool>(
                "DeleteDraft",
                &(DraftIdRequest {
                    storage: alice.clone(),
                    id: other_draft.id.clone()
                },)
            )
            .await
            .unwrap(),
        true
    );

    assert_eq!(
        proxy
            .call::<usize>(
                "UpsertSuggestions",
                &(UpsertSuggestionsRequest {
                    storage: alice.clone(),
                    suggestions: vec![suggestion("suggestion-a")],
                },)
            )
            .await
            .unwrap(),
        1
    );
    let listed_suggestions: Vec<FlowSuggestion> = proxy
        .call(
            "ListSuggestions",
            &(ListSuggestionsRequest {
                storage: alice.clone(),
                status: None,
                limit: 20,
            },),
        )
        .await
        .unwrap();
    assert_eq!(listed_suggestions.len(), 1);
    assert_eq!(
        proxy
            .call::<bool>(
                "SetSuggestionStatus",
                &(SetSuggestionStatusRequest {
                    storage: alice.clone(),
                    id: "suggestion-a".into(),
                    status: SuggestionStatus::Dismissed,
                },)
            )
            .await
            .unwrap(),
        true
    );
    assert!(
        proxy
            .call::<bool>(
                "SetSuggestionStatus",
                &(SetSuggestionStatusRequest {
                    storage: alice.clone(),
                    id: "missing".into(),
                    status: SuggestionStatus::Built,
                },)
            )
            .await
            .unwrap()
            == false
    );

    let created: PutWorkflowResponse = proxy
        .call(
            "PutWorkflow",
            &(PutWorkflowRequest::Create {
                storage: alice.clone(),
                name: "Run owner".into(),
                graph: graph("manual", false),
                require_approval: false,
            },),
        )
        .await
        .unwrap();
    let PutWorkflowResponse::Ok(created) = created else {
        panic!("workflow save failed")
    };
    let driver = FlowCatalogDocuments::new(alice_docs as Arc<dyn DocumentStore>);
    for (id, started) in [
        ("run-old", "2026-01-01T00:00:00Z"),
        ("run-new", "2026-01-02T00:00:00Z"),
    ] {
        driver
            .insert_flow_run(id, &created.workflow.id, id, started)
            .await
            .unwrap();
        driver
            .finish_flow_run(id, "completed", started, &[], &[], None, None)
            .await
            .unwrap();
    }
    let one: Option<FlowRun> = proxy
        .call(
            "GetRun",
            &(RunIdRequest {
                storage: alice.clone(),
                id: "run-new".into(),
            },),
        )
        .await
        .unwrap();
    assert_eq!(one.unwrap().id, "run-new");
    assert!(
        proxy
            .call::<Option<FlowRun>>(
                "GetRun",
                &(RunIdRequest {
                    storage: bob.clone(),
                    id: "run-new".into()
                },)
            )
            .await
            .unwrap()
            .is_none()
    );
    let per_flow: Vec<FlowRun> = proxy
        .call(
            "ListRuns",
            &(ListRunsRequest {
                storage: alice.clone(),
                flow_id: created.workflow.id.clone(),
                limit: 10,
            },),
        )
        .await
        .unwrap();
    assert_eq!(per_flow.len(), 2);
    let all: Vec<FlowRun> = proxy
        .call(
            "ListAllRuns",
            &(ListAllRunsRequest {
                storage: alice.clone(),
                limit: 10,
            },),
        )
        .await
        .unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(
        proxy
            .call::<usize>(
                "PruneRuns",
                &(PruneRunsRequest {
                    storage: alice.clone(),
                    flow_id: created.workflow.id.clone(),
                    keep: 1,
                },)
            )
            .await
            .unwrap(),
        1
    );
    let remaining: Vec<FlowRun> = proxy
        .call(
            "ListRuns",
            &(ListRunsRequest {
                storage: alice,
                flow_id: created.workflow.id,
                limit: 10,
            },),
        )
        .await
        .unwrap();
    assert_eq!(remaining.len(), 1);
}

#[tokio::test]
async fn promote_draft_calls_catalog_reentrantly_and_deletes_after_success() {
    let (_server, host) = connections().await;
    let (storage, _) = scope_with_documents(&host, "promote_success").await;
    let proxy = proxy(&host);
    let draft = create_draft(&proxy, storage.clone(), None).await;
    let callback = promotion_host(&host, storage.clone(), "success", false).await;
    let result: PromoteDraftResponse = proxy
        .call(
            "PromoteDraft",
            &(PromoteDraftRequest {
                storage: storage.clone(),
                host: callback,
                id: draft.id.clone(),
                require_approval: Some(true),
            },),
        )
        .await
        .unwrap();
    let PromoteDraftResponse::Ok(result) = result else {
        panic!("promotion failed: {result:?}")
    };
    assert_eq!(result.workflow.name, "Draft");
    assert_eq!(result.metadata["host_log"], "saved");
    assert!(result.draft_deleted);
    let saved: Option<catalog::Workflow> = proxy
        .call("GetWorkflow", &(storage.clone(), result.workflow.id))
        .await
        .unwrap();
    assert!(saved.is_some());
    let retained: Option<FlowDraft> = proxy
        .call(
            "GetDraft",
            &(DraftIdRequest {
                storage,
                id: draft.id,
            },),
        )
        .await
        .unwrap();
    assert!(retained.is_none());
}

#[tokio::test]
async fn promote_draft_preserves_host_failure_and_retains_draft() {
    let (_server, host) = connections().await;
    let (storage, _) = scope_with_documents(&host, "promote_failure").await;
    let proxy = proxy(&host);
    let draft = create_draft(&proxy, storage.clone(), None).await;
    let callback = promotion_host(&host, storage.clone(), "failure", true).await;
    let result: PromoteDraftResponse = proxy
        .call(
            "PromoteDraft",
            &(PromoteDraftRequest {
                storage: storage.clone(),
                host: callback,
                id: draft.id.clone(),
                require_approval: None,
            },),
        )
        .await
        .unwrap();
    let PromoteDraftResponse::Error(error) = result else {
        panic!("expected host error")
    };
    assert_eq!(error.code, "invalid_graph");
    assert_eq!(error.details.unwrap()["field"], "nodes[0]");
    let retained: Option<FlowDraft> = proxy
        .call(
            "GetDraft",
            &(DraftIdRequest {
                storage,
                id: draft.id,
            },),
        )
        .await
        .unwrap();
    assert!(retained.is_some());
}

#[tokio::test]
async fn promote_draft_reports_cleanup_failure_after_successful_save() {
    let (_server, host) = connections().await;
    let (storage, _) = scope_with_draft_delete_failure(&host, "promote_delete_failure").await;
    let proxy = proxy(&host);
    let draft = create_draft(&proxy, storage.clone(), None).await;
    let callback = promotion_host(&host, storage.clone(), "delete_failure", false).await;
    let result: PromoteDraftResponse = proxy
        .call(
            "PromoteDraft",
            &(PromoteDraftRequest {
                storage: storage.clone(),
                host: callback,
                id: draft.id.clone(),
                require_approval: None,
            },),
        )
        .await
        .unwrap();
    let PromoteDraftResponse::Ok(result) = result else {
        panic!("saved result was lost")
    };
    assert!(!result.draft_deleted);
    assert!(
        result
            .cleanup_warning
            .as_deref()
            .unwrap()
            .contains("injected draft delete failure")
    );
    let retained: Option<FlowDraft> = proxy
        .call(
            "GetDraft",
            &(DraftIdRequest {
                storage,
                id: draft.id,
            },),
        )
        .await
        .unwrap();
    assert!(retained.is_some());
}
