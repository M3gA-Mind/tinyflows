use super::graphs::graph;
use super::*;
use serde_json::Value;
use tinyflows_bus::catalog::{CatalogStorage, PutWorkflowRequest, PutWorkflowResponse};

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
