use super::*;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::sync::Arc;
use tinyflows_bus::catalog::{
    self, CallbackAddress, CatalogStorage, GetRevisionRequest, ListRevisionsRequest,
    PutWorkflowRequest, PutWorkflowResponse,
};
use tinystoragedrivers_core::{DocumentStore, MemoryStorage, Scope, StorageBackend};

struct HostDocuments(Arc<dyn DocumentStore>);

#[tinybus::interface(name = "ai.tinyhumans.tinyflows.CatalogStorage")]
impl HostDocuments {
    async fn doc_capabilities(&self, _request: Value) -> tinybus::Result<Value> {
        Ok(
            json!({"transactions":self.0.capabilities().contains(tinystoragedrivers_core::Capability::Transactions)}),
        )
    }

    async fn doc_ensure(&self, request: Value) -> tinybus::Result<Value> {
        Ok(reply(
            self.0.ensure_collection(&decode(&request["spec"])?).await,
        ))
    }

    async fn doc_get(&self, request: Value) -> tinybus::Result<Value> {
        Ok(reply(
            self.0
                .get(text(&request, "collection"), text(&request, "id"))
                .await,
        ))
    }

    async fn doc_put(&self, request: Value) -> tinybus::Result<Value> {
        let result = self
            .0
            .put(
                text(&request, "collection"),
                text(&request, "id"),
                request["doc"].clone(),
                decode(&request["precondition"])?,
            )
            .await;
        Ok(reply(result))
    }

    async fn doc_delete(&self, request: Value) -> tinybus::Result<Value> {
        let result = self
            .0
            .delete(
                text(&request, "collection"),
                text(&request, "id"),
                decode(&request["precondition"])?,
            )
            .await;
        Ok(reply(result))
    }

    async fn doc_query(&self, request: Value) -> tinybus::Result<Value> {
        Ok(reply(
            self.0
                .query(text(&request, "collection"), &decode(&request["query"])?)
                .await,
        ))
    }

    async fn doc_count(&self, request: Value) -> tinybus::Result<Value> {
        Ok(reply(
            self.0
                .count(text(&request, "collection"), &decode(&request["filter"])?)
                .await,
        ))
    }

    async fn doc_delete_where(&self, request: Value) -> tinybus::Result<Value> {
        Ok(reply(
            self.0
                .delete_where(text(&request, "collection"), &decode(&request["filter"])?)
                .await,
        ))
    }

    async fn doc_atomic_batch(&self, _request: Value) -> tinybus::Result<Value> {
        Err(tinybus::Error::failed("transactions are unavailable"))
    }
}

fn text<'a>(value: &'a Value, field: &str) -> &'a str {
    value[field].as_str().expect("well-formed document request")
}

fn decode<T: DeserializeOwned>(value: &Value) -> tinybus::Result<T> {
    serde_json::from_value(value.clone()).map_err(tinybus::Error::from)
}

fn reply(value: tinystoragedrivers_core::Result<impl Serialize>) -> Value {
    match value {
        Ok(value) => json!({"value":serde_json::to_value(value).unwrap()}),
        Err(error) => {
            json!({"error":{"kind":error.kind().to_string().replace(' ', "_"),"message":error.to_string()}})
        }
    }
}

fn graph(trigger_kind: &str, effect: bool) -> Value {
    let mut nodes = vec![
        json!({"id":"trigger","name":"Trigger","kind":"trigger","config":{"trigger_kind":trigger_kind}}),
        json!({"id":"agent","name":"Agent","kind":"agent","config":{"prompt":"Do the work"}}),
    ];
    let mut edges =
        vec![json!({"from_node":"trigger","from_port":"main","to_node":"agent","to_port":"main"})];
    if effect {
        nodes.push(json!({"id":"send","name":"Send","kind":"tool_call","config":{"slug":"provider:send","args":{}}}));
        edges.push(
            json!({"from_node":"agent","from_port":"main","to_node":"send","to_port":"main"}),
        );
    }
    json!({"nodes":nodes,"edges":edges})
}

async fn connections() -> (Connection, Connection) {
    let bus = tinybus::transport::memory::MemoryBus::new();
    tinybus::broker::Broker::new().spawn(bus.clone());
    let server = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    crate::service::setup(server.clone()).await.unwrap();
    let host = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    host.request_name("test.CatalogStorage").await.unwrap();
    (server, host)
}

async fn scope(host: &Connection, name: &str) -> CatalogStorage {
    let scoped = MemoryStorage::new().for_scope(&Scope::new(name)).unwrap();
    let path = format!("/test/catalog/{name}");
    host.serve_at(
        path.parse().unwrap(),
        HostDocuments(scoped.documents().clone()),
    )
    .await
    .unwrap();
    CatalogStorage {
        callback: CallbackAddress {
            destination: "test.CatalogStorage".into(),
            object_path: path,
        },
    }
}

fn put_proxy(host: &Connection) -> tinybus::Result<tinybus::Proxy> {
    host.proxy(catalog::BUS_NAME, catalog::OBJECT_PATH, catalog::INTERFACE)
}

#[tokio::test]
async fn catalog_bus_serves_create_update_conflict_history_and_agent_scopes() {
    let (_server, host) = connections().await;
    let alice = scope(&host, "alice").await;
    let bob = scope(&host, "bob").await;
    let proxy = put_proxy(&host).unwrap();

    let created: PutWorkflowResponse = proxy
        .call(
            "PutWorkflow",
            &(PutWorkflowRequest::Create {
                storage: alice.clone(),
                name: "Manual flow".into(),
                graph: graph("manual", false),
                require_approval: false,
            }),
        )
        .await
        .unwrap();
    let PutWorkflowResponse::Ok(created) = created else {
        panic!("create failed: {created:?}")
    };
    assert!(!created.born_disabled);
    assert!(!created.approval_forced);
    let id = created.workflow.id.clone();
    let version = created.workflow.updated_at.clone();

    let changed: PutWorkflowResponse = proxy
        .call(
            "PutWorkflow",
            &(PutWorkflowRequest::Update {
                storage: alice.clone(),
                id: id.clone(),
                name: "Updated flow".into(),
                graph: graph("manual", false),
                require_approval: false,
                expected_version: Some(version.clone()),
                force_disarm_automatic: false,
            }),
        )
        .await
        .unwrap();
    let PutWorkflowResponse::Ok(changed) = changed else {
        panic!("update failed: {changed:?}")
    };
    let conflict: PutWorkflowResponse = proxy
        .call(
            "PutWorkflow",
            &(PutWorkflowRequest::Update {
                storage: alice.clone(),
                id: id.clone(),
                name: "Stale edit".into(),
                graph: graph("manual", false),
                require_approval: false,
                expected_version: Some(version),
                force_disarm_automatic: false,
            }),
        )
        .await
        .unwrap();
    assert!(
        matches!(conflict, PutWorkflowResponse::Error(catalog::FlowUpdateError::VersionConflict { ref current, .. }) if current.name == "Updated flow")
    );

    let revisions: Vec<WorkflowRevision> = proxy
        .call(
            "ListRevisions",
            &(ListRevisionsRequest {
                storage: alice.clone(),
                flow_id: id.clone(),
                limit: 10,
            }),
        )
        .await
        .unwrap();
    assert_eq!(revisions.len(), 1);
    let fetched: Option<WorkflowRevision> = proxy
        .call(
            "GetRevision",
            &(GetRevisionRequest {
                storage: alice.clone(),
                flow_id: id.clone(),
                revision_id: revisions[0].id.clone(),
            }),
        )
        .await
        .unwrap();
    assert_eq!(fetched, Some(revisions[0].clone()));

    let alice_list: ListWorkflowsResult = proxy.call("ListWorkflows", &(alice,)).await.unwrap();
    assert_eq!(alice_list.workflows[0].name, "Updated flow");
    assert_eq!(alice_list.workflows[0].revision, 2);
    assert_eq!(
        alice_list.workflows[0].trigger_kind.as_deref(),
        Some("manual")
    );
    assert_eq!(alice_list.workflows[0].node_count, 2);
    let bob_list: ListWorkflowsResult = proxy.call("ListWorkflows", &(bob,)).await.unwrap();
    assert!(bob_list.workflows.is_empty());

    let enabled: Workflow = proxy
        .call(
            "SetEnabled",
            &(catalog::SetEnabledRequest {
                storage: alice.clone(),
                id: id.clone(),
                enabled: true,
            }),
        )
        .await
        .unwrap();
    assert!(enabled.enabled);
    let duplicate: PutWorkflowResponse = proxy
        .call(
            "PutWorkflow",
            &(PutWorkflowRequest::Duplicate {
                storage: alice.clone(),
                id: id.clone(),
                name: "Updated flow copy".into(),
            }),
        )
        .await
        .unwrap();
    let PutWorkflowResponse::Ok(duplicate) = duplicate else {
        panic!("duplicate failed: {duplicate:?}")
    };
    assert!(duplicate.born_disabled);
    assert!(!duplicate.workflow.enabled);
    let deleted: catalog::DeleteWorkflowResult = proxy
        .call("DeleteWorkflow", &(alice, &duplicate.workflow.id))
        .await
        .unwrap();
    assert!(deleted.removed);
}

#[tokio::test]
async fn catalog_create_reports_disabled_and_forced_approval_safety_rules() {
    let (_server, host) = connections().await;
    let storage = scope(&host, "policy").await;
    let proxy = put_proxy(&host).unwrap();
    let result: PutWorkflowResponse = proxy
        .call(
            "PutWorkflow",
            &(PutWorkflowRequest::Create {
                storage,
                name: "Scheduled side effect".into(),
                graph: graph("schedule", true),
                require_approval: false,
            }),
        )
        .await
        .unwrap();
    let PutWorkflowResponse::Ok(result) = result else {
        panic!("create failed: {result:?}")
    };
    assert!(result.born_disabled);
    assert!(result.approval_forced);
    assert!(!result.workflow.enabled);
    assert!(result.workflow.require_approval);
}
