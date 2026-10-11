use super::*;
use serde_json::json;

#[tokio::test]
async fn binding_failures_excludes_unrelated_authoring_gates() -> Result<()> {
    let failures = Authoring
        .binding_failures(json!({
            "name": "test", "edges": [], "nodes": [
                {"id": "code", "name": "Code", "kind": "code",
                 "config": {"language": "shell"}},
                {"id": "agent", "name": "Agent", "kind": "agent",
                 "config": {"prompt": "=Look at the diff and fix it",
                            "output_parser": {"schema": {"properties": {}}}}},
                {"id": "notify", "name": "Notify", "kind": "tool_call",
                 "config": {"slug": "demo:echo",
                            "args": {"text": "=nodes.agent.item.json.title"}}}
            ]
        }))
        .await?;
    assert_eq!(failures, Vec::<String>::new());
    Ok(())
}

#[tokio::test]
async fn binding_failures_retains_missing_output_envelope_diagnostics() -> Result<()> {
    let failures = Authoring
        .binding_failures(json!({
            "name": "test", "edges": [], "nodes": [
                {"id": "fetch", "name": "Fetch", "kind": "agent",
                 "config": {"prompt": "Fetch the title"}},
                {"id": "notify", "name": "Notify", "kind": "tool_call",
                 "config": {"slug": "demo:echo",
                            "args": {"text": "=nodes.fetch.item.title"}}}
            ]
        }))
        .await?;
    assert_eq!(failures.len(), 1, "{failures:?}");
    assert!(failures[0].contains("=nodes.fetch.item.json.title"));
    Ok(())
}

#[tokio::test]
async fn bus_dispatch_returns_typed_validation_report() {
    let bus = tinybus::transport::memory::MemoryBus::new();
    tinybus::broker::Broker::new().spawn(bus.clone());
    let server = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    setup(server.clone()).await.unwrap();
    let client = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let proxy = client
        .proxy(BUS_NAME, OBJECT_PATH, tinyflows_bus::INTERFACE)
        .unwrap();
    let report: GraphValidation = proxy
        .call(
            tinyflows_bus::methods::VALIDATE_GRAPH,
            &(json!({"nodes": "invalid"}),),
        )
        .await
        .unwrap();
    assert_eq!(report.errors[0].code, "unparseable_graph");
    assert!(!report.is_valid());
}
