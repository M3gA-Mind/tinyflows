use super::*;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tinybus::module::ModuleHost;
use tinybus::transport::memory::MemoryTransport;
use tinyflows_bus::{
    BUS_NAME,
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

struct AgentCallback {
    started: Notify,
    release: Notify,
    calls: Arc<AtomicUsize>,
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

#[tokio::test]
#[ignore = "run through scripts/verify-module-artifact.sh"]
async fn compiled_module_runs_scoped_graphs_and_streams_persisted_progress() {
    let artifact = std::env::var_os("TINYFLOWS_MODULE_ARTIFACT")
        .expect("set TINYFLOWS_MODULE_ARTIFACT to the built cdylib");
    let broker = tinybus::broker::Broker::new();
    let module_host = ModuleHost::new(broker.clone());
    module_host
        .load_file(std::path::PathBuf::from(artifact))
        .unwrap();
    let (client, transport) = MemoryTransport::pair();
    broker.attach(Arc::new(transport));
    let connection = Connection::connect(Box::new(client)).await.unwrap();
    let catalog_proxy = connection
        .proxy(BUS_NAME, catalog::OBJECT_PATH, catalog::INTERFACE)
        .unwrap();
    let runs_proxy = connection
        .proxy(BUS_NAME, runs::OBJECT_PATH, runs::INTERFACE)
        .unwrap();

    let alice = owner(&connection, "alice").await;
    let bob = owner(&connection, "bob").await;
    let alice_flow = create_flow(&catalog_proxy, &alice.storage, "Alice").await;
    let bob_flow = create_flow(&catalog_proxy, &bob.storage, "Bob").await;
    let alice_agent = serve_agent(&connection, "alice").await;
    let bob_agent = serve_agent(&connection, "bob").await;
    let run_id = "same-opaque-run-id";
    let alice_start = request(&alice, alice_flow, run_id, graph(true), 20_000);
    let bob_start = request(&bob, bob_flow, run_id, graph(true), 20_000);

    let invalid = request(
        &alice,
        alice_start.context.flow_id.clone(),
        "preflight",
        graph(false),
        20_000,
    );
    let error = runs_proxy
        .call::<runs::StartRunResponse>("StartRun", (invalid,))
        .await;
    assert!(
        error.is_err(),
        "missing declared input must reject synchronously"
    );
    let missing = catalog_proxy
        .call::<Option<catalog::FlowRun>>(
            "GetRun",
            (RunIdRequest {
                storage: alice.storage.clone(),
                id: "preflight".into(),
            },),
        )
        .await;
    assert_eq!(
        missing.unwrap(),
        None,
        "preflight failure must leave no row"
    );

    let started: runs::StartRunResponse = runs_proxy
        .call("StartRun", (alice_start.clone(),))
        .await
        .unwrap();
    assert_eq!(started.run_id, run_id);
    assert!(!started.already_running);
    alice_agent.started.notified().await;

    let duplicate: runs::StartRunResponse = runs_proxy
        .call("StartRun", (alice_start.clone(),))
        .await
        .unwrap();
    assert!(duplicate.already_running);
    assert_eq!(alice_agent.calls.load(Ordering::SeqCst), 1);

    let bob_started: runs::StartRunResponse =
        runs_proxy.call("StartRun", (bob_start,)).await.unwrap();
    assert!(!bob_started.already_running);
    bob_agent.started.notified().await;
    assert_eq!(bob_agent.calls.load(Ordering::SeqCst), 1);

    let wrong_owner = RunControlRequest {
        storage: owner(&connection, "mallory").await.storage,
        run_id: run_id.into(),
        cursor: 0,
    };
    assert!(
        runs_proxy
            .call::<runs::PollRun>("PollRun", (wrong_owner,))
            .await
            .is_err()
    );

    let alice_cancel: runs::CancelRunResponse = runs_proxy
        .call(
            "CancelRun",
            (RunControlRequest {
                storage: alice.storage.clone(),
                run_id: run_id.into(),
                cursor: 0,
            },),
        )
        .await
        .unwrap();
    assert!(alice_cancel.signalled);
    alice_agent.release.notify_one();
    bob_agent.release.notify_one();

    let alice_poll = poll_until_done(&runs_proxy, &alice.storage, run_id).await;
    let bob_poll = poll_until_done(&runs_proxy, &bob.storage, run_id).await;
    assert!(
        alice_poll
            .events
            .iter()
            .any(|event| event.kind == "agent_progress")
    );
    assert!(
        alice_poll
            .events
            .iter()
            .any(|event| event.kind == "graph_observation")
    );
    assert!(alice_poll.events.iter().any(|event| event.kind == "step"));
    assert_eq!(
        alice_poll.events.last().unwrap().data["status"],
        "cancelled"
    );
    assert!(bob_poll.events.iter().any(|event| event.kind == "finished"));

    let alice_row: Option<catalog::FlowRun> = catalog_proxy
        .call(
            "GetRun",
            (RunIdRequest {
                storage: alice.storage.clone(),
                id: run_id.into(),
            },),
        )
        .await
        .unwrap();
    let bob_row: Option<catalog::FlowRun> = catalog_proxy
        .call(
            "GetRun",
            (RunIdRequest {
                storage: bob.storage.clone(),
                id: run_id.into(),
            },),
        )
        .await
        .unwrap();
    let alice_row = alice_row.expect("alice run row persisted");
    let bob_row = bob_row.expect("bob run row persisted");
    assert_eq!(alice_row.status, "cancelled");
    assert_eq!(bob_row.status, "completed");
    assert!(alice_row.steps.iter().any(|step| step.node_id == "agent"));
    assert!(bob_row.steps.iter().any(|step| step.node_id == "agent"));
    let output = bob_row
        .steps
        .iter()
        .find(|step| step.node_id == "output")
        .expect("downstream transform ran");
    assert_eq!(output.output[0]["json"]["declared"], "declared-value");
    assert_eq!(output.output[0]["json"]["payload"], "trigger-value");
    let terminal_cancel: runs::CancelRunResponse = runs_proxy
        .call(
            "CancelRun",
            (RunControlRequest {
                storage: alice.storage.clone(),
                run_id: run_id.into(),
                cursor: 0,
            },),
        )
        .await
        .unwrap();
    assert!(!terminal_cancel.signalled);
}

struct Owner {
    storage: CatalogStorage,
    caps: runs::CallbackAddress,
}

async fn owner(connection: &Connection, name: &str) -> Owner {
    let scoped = MemoryStorage::new()
        .for_scope(&Scope::new(name).unwrap())
        .unwrap();
    let path = format!("/test/catalog/{name}");
    connection
        .serve_at(
            path.parse().unwrap(),
            OwnerDocuments {
                docs: scoped.documents().clone(),
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

async fn serve_agent(connection: &Connection, name: &str) -> Arc<AgentCallback> {
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

async fn create_flow(proxy: &tinybus::Proxy, storage: &CatalogStorage, name: &str) -> String {
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

fn graph(include_agent: bool) -> Value {
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

fn request(
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

async fn poll_until_done(
    proxy: &tinybus::Proxy,
    storage: &CatalogStorage,
    run_id: &str,
) -> runs::PollRun {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let mut cursor = 0;
        let mut all = Vec::new();
        loop {
            let page: runs::PollRun = proxy
                .call(
                    "PollRun",
                    (RunControlRequest {
                        storage: storage.clone(),
                        run_id: run_id.into(),
                        cursor,
                    },),
                )
                .await
                .unwrap();
            cursor = page.next_cursor;
            all.extend(page.events);
            if page.done {
                return runs::PollRun {
                    events: all,
                    next_cursor: cursor,
                    done: true,
                };
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("run actor should flush terminal events")
}
