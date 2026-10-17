use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tinybus::Connection;
use tinyflows_bus::{
    catalog::{self, CatalogStorage, PutWorkflowRequest, PutWorkflowResponse, RunIdRequest},
    runs::{
        self, AgentDefinition, AgentEventsRequest, AgentEventsResponse, AgentProgressEvent,
        AgentRunHandle, AgentRunOutcome, AgentRunRequest, FlowRunTrigger, RunContext,
        RunControlRequest, StartRun,
    },
};
use tinystoragedrivers_core::{DocumentStore, MemoryStorage, Scope, StorageBackend};
use tokio::sync::Notify;

struct OwnerDocuments {
    docs: Arc<dyn DocumentStore>,
    terminal_write_started: Option<Arc<Notify>>,
    release_terminal_write: Option<Arc<Notify>>,
    fail_run_insert: Option<Arc<AtomicBool>>,
    run_insert_started: Option<Arc<Notify>>,
    release_run_insert: Option<Arc<Notify>>,
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
                release.notified().await;
            }
            return Ok(json!({
                "error": {
                    "kind": "unavailable",
                    "message": "injected run-row insert failure"
                }
            }));
        }
        if let Some(events_json) = request["doc"]["events_json"].as_str()
            && serde_json::from_str::<Vec<Value>>(events_json)
                .ok()
                .and_then(|events| events.last().cloned())
                .is_some_and(|event| event["kind"] == "finished")
            && let (Some(started), Some(release)) =
                (&self.terminal_write_started, &self.release_terminal_write)
        {
            started.notify_one();
            release.notified().await;
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
    pub(super) calls: Arc<AtomicUsize>,
}

#[tinybus::interface(name = "ai.tinyhumans.tinyflows.CapsHost")]
impl AgentCallback {
    async fn resolve_agent(&self, _agent_ref: String) -> tinybus::Result<Option<AgentDefinition>> {
        Ok(None)
    }

    async fn agent_run(&self, _request: AgentRunRequest) -> tinybus::Result<AgentRunHandle> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.started.notify_one();
        self.release.notified().await;
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
}

pub(super) async fn owner(
    connection: &Connection,
    name: &str,
    terminal_gate: Option<(Arc<Notify>, Arc<Notify>)>,
    run_insert_gate: Option<(Arc<AtomicBool>, Arc<Notify>, Arc<Notify>)>,
) -> Owner {
    let scoped = MemoryStorage::new()
        .for_scope(&Scope::new(name).unwrap())
        .unwrap();
    let path = format!("/test/catalog/{name}");
    connection
        .serve_at(
            path.parse().unwrap(),
            OwnerDocuments {
                docs: scoped.documents().clone(),
                terminal_write_started: terminal_gate.as_ref().map(|(started, _)| started.clone()),
                release_terminal_write: terminal_gate.map(|(_, release)| release),
                fail_run_insert: run_insert_gate.as_ref().map(|(fail, _, _)| fail.clone()),
                run_insert_started: run_insert_gate
                    .as_ref()
                    .map(|(_, started, _)| started.clone()),
                release_run_insert: run_insert_gate.map(|(_, _, release)| release),
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
            object_path: format!("/test/caps/{name}"),
        },
    }
}

pub(super) async fn serve_agent(connection: &Connection, name: &str) -> Arc<AgentCallback> {
    let callback = Arc::new(AgentCallback {
        started: Notify::new(),
        release: Notify::new(),
        calls: Arc::new(AtomicUsize::new(0)),
    });
    let path = format!("/test/caps/{name}");
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
    let response: PutWorkflowResponse = proxy
        .call(
            "PutWorkflow",
            &(PutWorkflowRequest::Create {
                storage: storage.clone(),
                name: name.into(),
                graph: graph(true),
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

pub(super) fn graph(include_agent: bool) -> Value {
    let mut nodes = vec![json!({"id":"trigger","name":"Trigger","kind":"trigger","config":{}})];
    let mut edges = Vec::new();
    if include_agent {
        nodes.push(json!({"id":"agent","name":"Agent","kind":"agent","config":{"agent_ref":"test-agent","prompt":"work"}}));
        nodes.push(json!({"id":"output","name":"Output","kind":"transform","config":{"set":{"declared":"=inputs.declared","payload":"=run.trigger.payload"}}}));
        edges.push(
            json!({"from_node":"trigger","from_port":"main","to_node":"agent","to_port":"main"}),
        );
        edges.push(
            json!({"from_node":"agent","from_port":"main","to_node":"output","to_port":"main"}),
        );
    }
    json!({
        "inputs":[{"name":"declared","type":"string","required":true}],
        "nodes":nodes,
        "edges":edges
    })
}

pub(super) fn request(
    owner: &Owner,
    flow_id: String,
    run_id: &str,
    graph: Value,
    timeout_ms: u64,
) -> StartRun {
    let mut inputs = serde_json::Map::new();
    inputs.insert("declared".into(), json!("declared-value"));
    StartRun {
        run_id: run_id.into(),
        context: RunContext {
            flow_id,
            origin: Some(json!({"source":"artifact-test"})),
        },
        caps_host: runs::CapsHost {
            callback: owner.caps.clone(),
        },
        storage: owner.storage.clone(),
        graph,
        trigger_kind: FlowRunTrigger::Rpc,
        trigger_data: json!({"payload":"trigger-value"}),
        inputs,
        timeout_ms,
    }
}
