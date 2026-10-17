use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tinybus::Connection;
use tinyflows_bus::{
    catalog::{self, CatalogStorage, PutWorkflowRequest, PutWorkflowResponse},
    runs::{
        self, AgentDefinition, AgentEventsRequest, AgentEventsResponse, AgentProgressEvent,
        AgentRunHandle, AgentRunOutcome, AgentRunRequest, FlowRunTrigger, RunContext, StartRun,
    },
};
use tinystoragedrivers_core::{DocumentStore, MemoryStorage, Scope, StorageBackend};
use tokio::sync::Notify;

#[path = "graphs.rs"]
mod graphs;
pub(super) use graphs::{dedup_graph, graph, parked_graph, request};

struct OwnerDocuments {
    docs: Arc<dyn DocumentStore>,
    terminal_write_started: Option<Arc<Notify>>,
    release_terminal_write: Option<Arc<Notify>>,
    terminal_write_pending: Option<Arc<AtomicBool>>,
    fail_run_insert: Option<Arc<AtomicBool>>,
    run_insert_started: Option<Arc<Notify>>,
    release_run_insert: Option<Arc<Notify>>,
    fail_snapshot: Option<Arc<AtomicBool>>,
    fail_checkpoint_delete: Option<Arc<AtomicBool>>,
    fail_parked_expiry: Option<(String, Arc<AtomicBool>)>,
    cleanup_started: Option<Arc<Notify>>,
    release_cleanup: Option<Arc<Notify>>,
    cleanup_pending: Option<Arc<AtomicBool>>,
}

#[tinybus::interface(name = "ai.tinyhumans.tinyflows.CatalogStorage")]
impl OwnerDocuments {
    async fn doc_ensure(&self, request: Value) -> tinybus::Result<Value> {
        Ok(reply(
            self.docs
                .ensure_collection(&decode(&request["spec"])?)
                .await,
        ))
    }

    async fn doc_get(&self, request: Value) -> tinybus::Result<Value> {
        Ok(reply(
            self.docs
                .get(text(&request, "collection"), text(&request, "id"))
                .await,
        ))
    }

    async fn doc_put(&self, request: Value) -> tinybus::Result<Value> {
        if request["collection"] == "flows_runs"
            && request["doc"]["status"] == "cancelled"
            && request["doc"]["error"]
                .as_str()
                .is_some_and(|error| error.starts_with("parked run expired"))
            && self.fail_parked_expiry.as_ref().is_some_and(|(id, fail)| {
                id == text(&request, "id") && fail.swap(false, Ordering::SeqCst)
            })
        {
            return Ok(json!({
                "error": {
                    "kind": "unavailable",
                    "message": "injected parked expiry failure"
                }
            }));
        }
        if request["collection"] == "flows_runs"
            && request["doc"]["status"] == "running"
            && self
                .fail_run_insert
                .as_ref()
                .is_some_and(|fail| fail.swap(false, Ordering::SeqCst))
        {
            if let Some(started) = &self.run_insert_started {
                started.notify_one();
            }
            if let Some(release) = &self.release_run_insert {
                let _ =
                    tokio::time::timeout(std::time::Duration::from_secs(10), release.notified())
                        .await;
            }
            return Ok(json!({
                "error": {
                    "kind": "unavailable",
                    "message": "injected run-row insert failure"
                }
            }));
        }
        if request["collection"] == "flows_runs"
            && request["doc"].get("execution_snapshot").is_some()
            && self
                .fail_snapshot
                .as_ref()
                .is_some_and(|fail| fail.swap(false, Ordering::SeqCst))
        {
            return Ok(json!({
                "error": {
                    "kind": "unavailable",
                    "message": "injected execution-snapshot failure"
                }
            }));
        }
        if let Some(events_json) = request["doc"]["events_json"].as_str()
            && serde_json::from_str::<Vec<Value>>(events_json)
                .ok()
                .and_then(|events| events.last().cloned())
                .is_some_and(|event| event["kind"] == "finished")
            && self
                .terminal_write_pending
                .as_ref()
                .is_some_and(|pending| pending.swap(false, Ordering::SeqCst))
            && let (Some(started), Some(release)) =
                (&self.terminal_write_started, &self.release_terminal_write)
        {
            started.notify_one();
            let _ =
                tokio::time::timeout(std::time::Duration::from_secs(10), release.notified()).await;
        }
        Ok(reply(
            self.docs
                .put(
                    text(&request, "collection"),
                    text(&request, "id"),
                    request["doc"].clone(),
                    decode(&request["precondition"])?,
                )
                .await,
        ))
    }

    async fn doc_delete(&self, request: Value) -> tinybus::Result<Value> {
        if request["collection"] == "flows_kv"
            && self
                .cleanup_pending
                .as_ref()
                .is_some_and(|pending| pending.swap(false, Ordering::SeqCst))
            && let (Some(started), Some(release)) = (&self.cleanup_started, &self.release_cleanup)
        {
            started.notify_one();
            let _ = tokio::time::timeout(
                std::time::Duration::from_secs(10),
                release.notified(),
            )
            .await;
        }
        if request["collection"] == "flows_graph_checkpoints"
            && self
                .fail_checkpoint_delete
                .as_ref()
                .is_some_and(|fail| fail.swap(false, Ordering::SeqCst))
        {
            return Ok(json!({
                "error": {
                    "kind": "unavailable",
                    "message": "injected checkpoint delete failure"
                }
            }));
        }
        Ok(reply(
            self.docs
                .delete(
                    text(&request, "collection"),
                    text(&request, "id"),
                    decode(&request["precondition"])?,
                )
                .await,
        ))
    }

    async fn doc_query(&self, request: Value) -> tinybus::Result<Value> {
        Ok(reply(
            self.docs
                .query(text(&request, "collection"), &decode(&request["query"])?)
                .await,
        ))
    }

    async fn doc_delete_where(&self, request: Value) -> tinybus::Result<Value> {
        if request["collection"] == "flows_graph_checkpoints"
            && self
                .fail_checkpoint_delete
                .as_ref()
                .is_some_and(|fail| fail.swap(false, Ordering::SeqCst))
        {
            return Ok(json!({
                "error": {
                    "kind": "unavailable",
                    "message": "injected checkpoint delete-where failure"
                }
            }));
        }
        Ok(reply(
            self.docs
                .delete_where(text(&request, "collection"), &decode(&request["filter"])?)
                .await,
        ))
    }
}

fn text<'a>(value: &'a Value, field: &str) -> &'a str {
    value[field]
        .as_str()
        .expect("valid document callback field")
}

fn decode<T: DeserializeOwned>(value: &Value) -> tinybus::Result<T> {
    serde_json::from_value(value.clone()).map_err(tinybus::Error::from)
}

fn reply(value: tinystoragedrivers_core::Result<impl Serialize>) -> Value {
    match value {
        Ok(value) => json!({"value":serde_json::to_value(value).unwrap()}),
        Err(error) => json!({
            "error": {
                "kind": error.kind().to_string().replace(' ', "_"),
                "message": error.to_string()
            }
        }),
    }
}

pub(super) struct AgentCallback {
    pub(super) started: Notify,
    pub(super) release: Notify,
    pub(super) approval_started: Notify,
    pub(super) calls: Arc<AtomicUsize>,
    pub(super) fail_start: bool,
}

#[tinybus::interface(name = "ai.tinyhumans.tinyflows.CapsHost")]
impl AgentCallback {
    async fn resolve_agent(&self, _agent_ref: String) -> tinybus::Result<Option<AgentDefinition>> {
        Ok(None)
    }

    async fn resolve_tools(
        &self,
        _request: runs::ResolveToolsRequest,
    ) -> tinybus::Result<Vec<runs::ToolDescriptor>> {
        Ok(Vec::new())
    }

    async fn approval_decide(
        &self,
        _request: runs::ApprovalRequest,
    ) -> tinybus::Result<runs::ApprovalOutcome> {
        self.approval_started.notify_one();
        Ok(runs::ApprovalOutcome::Pending)
    }

    async fn approval_cancel(&self, _request: runs::ApprovalCancelRequest) -> tinybus::Result<()> {
        Ok(())
    }

    async fn agent_run(&self, _request: AgentRunRequest) -> tinybus::Result<AgentRunHandle> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.started.notify_one();
        if self.fail_start {
            return Err(tinybus::Error::failed("injected agent callback failure"));
        }
        let _ =
            tokio::time::timeout(std::time::Duration::from_secs(10), self.release.notified()).await;
        Ok(AgentRunHandle {
            handle: "agent-handle".into(),
        })
    }

    async fn agent_events(
        &self,
        _request: AgentEventsRequest,
    ) -> tinybus::Result<AgentEventsResponse> {
        Ok(AgentEventsResponse {
            events: vec![AgentProgressEvent {
                seq: 1,
                at_ms: 1,
                kind: "thinking".into(),
                text: "working".into(),
            }],
            done: true,
            outcome: Some(AgentRunOutcome {
                stop: runs::StopReason::Finished,
                text: Some("done".into()),
                json: json!({}),
                raw: json!({}),
                usage: None,
                transcript: Vec::new(),
            }),
        })
    }
}

pub(super) struct Owner {
    pub(super) storage: CatalogStorage,
    pub(super) caps: runs::CallbackAddress,
    pub(super) docs: Arc<dyn DocumentStore>,
}

pub(super) async fn owner(
    connection: &Connection,
    name: &str,
    terminal_gate: Option<(Arc<Notify>, Arc<Notify>)>,
    run_insert_gate: Option<(Arc<AtomicBool>, Arc<Notify>, Arc<Notify>)>,
    fail_snapshot: Option<Arc<AtomicBool>>,
    fail_checkpoint_delete: Option<Arc<AtomicBool>>,
) -> Owner {
    let scoped = MemoryStorage::new()
        .for_scope(&Scope::new(name).unwrap())
        .unwrap();
    let docs = scoped.documents().clone();
    let path = format!("/test/catalog/{}", name.replace('-', "_"));
    connection
        .serve_at(
            path.parse().unwrap(),
            OwnerDocuments {
                docs: docs.clone(),
                terminal_write_started: terminal_gate.as_ref().map(|(started, _)| started.clone()),
                release_terminal_write: terminal_gate.as_ref().map(|(_, release)| release.clone()),
                terminal_write_pending: terminal_gate
                    .as_ref()
                    .map(|_| Arc::new(AtomicBool::new(true))),
                fail_run_insert: run_insert_gate.as_ref().map(|(fail, _, _)| fail.clone()),
                run_insert_started: run_insert_gate
                    .as_ref()
                    .map(|(_, started, _)| started.clone()),
                release_run_insert: run_insert_gate.map(|(_, _, release)| release),
                fail_snapshot,
                fail_checkpoint_delete,
                fail_parked_expiry: None,
                cleanup_started: None,
                release_cleanup: None,
                cleanup_pending: None,
            },
        )
        .await
        .unwrap();
    connection
        .request_name("test.CatalogStorage")
        .await
        .unwrap();
    Owner {
        storage: CatalogStorage {
            callback: catalog::CallbackAddress {
                destination: "test.CatalogStorage".into(),
                object_path: path,
            },
        },
        caps: runs::CallbackAddress {
            destination: "test.CapsHost".into(),
            object_path: format!("/test/caps/{}", name.replace('-', "_")),
        },
        docs,
    }
}

pub(super) async fn alias_owner(
    connection: &Connection,
    name: &str,
    docs: Arc<dyn DocumentStore>,
) -> Owner {
    let path = format!("/test/catalog_alias/{}", name.replace('-', "_"));
    connection
        .serve_at(
            path.parse().unwrap(),
            OwnerDocuments {
                docs: docs.clone(),
                terminal_write_started: None,
                release_terminal_write: None,
                terminal_write_pending: None,
                fail_run_insert: None,
                run_insert_started: None,
                release_run_insert: None,
                fail_snapshot: None,
                fail_checkpoint_delete: None,
                fail_parked_expiry: None,
                cleanup_started: None,
                release_cleanup: None,
                cleanup_pending: None,
            },
        )
        .await
        .unwrap();
    Owner {
        storage: CatalogStorage {
            callback: catalog::CallbackAddress {
                destination: "test.CatalogStorage".into(),
                object_path: path,
            },
        },
        caps: runs::CallbackAddress {
            destination: "test.CapsHost".into(),
            object_path: format!("/test/caps/{}", name.replace('-', "_")),
        },
        docs,
    }
}

pub(super) async fn owner_with_lifecycle_gates(
    connection: &Connection,
    name: &str,
    expiry_failure_id: Option<&str>,
    cleanup_gate: Option<(Arc<Notify>, Arc<Notify>)>,
) -> Owner {
    let scoped = MemoryStorage::new()
        .for_scope(&Scope::new(name).unwrap())
        .unwrap();
    let docs = scoped.documents().clone();
    let path = format!("/test/catalog/{}", name.replace('-', "_"));
    let expiry_failure = expiry_failure_id.map(|id| (id.to_owned(), Arc::new(AtomicBool::new(true))));
    connection
        .serve_at(
            path.parse().unwrap(),
            OwnerDocuments {
                docs: docs.clone(),
                terminal_write_started: None,
                release_terminal_write: None,
                terminal_write_pending: None,
                fail_run_insert: None,
                run_insert_started: None,
                release_run_insert: None,
                fail_snapshot: None,
                fail_checkpoint_delete: None,
                fail_parked_expiry: expiry_failure,
                cleanup_started: cleanup_gate.as_ref().map(|(started, _)| started.clone()),
                release_cleanup: cleanup_gate.as_ref().map(|(_, release)| release.clone()),
                cleanup_pending: cleanup_gate
                    .as_ref()
                    .map(|_| Arc::new(AtomicBool::new(true))),
            },
        )
        .await
        .unwrap();
    connection
        .request_name("test.CatalogStorage")
        .await
        .unwrap();
    Owner {
        storage: CatalogStorage {
            callback: catalog::CallbackAddress {
                destination: "test.CatalogStorage".into(),
                object_path: path,
            },
        },
        caps: runs::CallbackAddress {
            destination: "test.CapsHost".into(),
            object_path: format!("/test/caps/{}", name.replace('-', "_")),
        },
        docs,
    }
}

pub(super) async fn serve_agent(connection: &Connection, name: &str) -> Arc<AgentCallback> {
    serve_agent_with_failure(connection, name, false).await
}

pub(super) async fn serve_failing_agent(connection: &Connection, name: &str) -> Arc<AgentCallback> {
    serve_agent_with_failure(connection, name, true).await
}

async fn serve_agent_with_failure(
    connection: &Connection,
    name: &str,
    fail_start: bool,
) -> Arc<AgentCallback> {
    let callback = Arc::new(AgentCallback {
        started: Notify::new(),
        release: Notify::new(),
        approval_started: Notify::new(),
        calls: Arc::new(AtomicUsize::new(0)),
        fail_start,
    });
    let path = format!("/test/caps/{}", name.replace('-', "_"));
    connection
        .serve_at(path.parse().unwrap(), callback.clone())
        .await
        .unwrap();
    connection.request_name("test.CapsHost").await.unwrap();
    callback
}

pub(super) async fn create_flow(
    proxy: &tinybus::Proxy,
    storage: &CatalogStorage,
    name: &str,
) -> String {
    create_flow_with_graph(proxy, storage, name, graph(true)).await
}

pub(super) async fn create_flow_with_graph(
    proxy: &tinybus::Proxy,
    storage: &CatalogStorage,
    name: &str,
    graph: Value,
) -> String {
    let response: PutWorkflowResponse = proxy
        .call(
            "PutWorkflow",
            &(PutWorkflowRequest::Create {
                storage: storage.clone(),
                name: name.into(),
                graph,
                require_approval: false,
            }),
        )
        .await
        .unwrap();
    let PutWorkflowResponse::Ok(created) = response else {
        panic!("failed to create flow: {response:?}");
    };
    created.workflow.id
}

pub(super) async fn wait_notify(notify: &Notify, boundary: &str) {
    tokio::time::timeout(std::time::Duration::from_secs(10), notify.notified())
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {boundary}"));
}
