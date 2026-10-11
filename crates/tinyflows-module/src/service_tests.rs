use super::*;
use serde_json::json;

#[tokio::test]
async fn binding_failures_excludes_unrelated_authoring_gates() -> Result<()> {
    let (server, _client) = authoring_connections().await?;
    let failures = Authoring { connection: server }
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
    let (server, _client) = authoring_connections().await?;
    let failures = Authoring { connection: server }
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

async fn authoring_connections() -> Result<(Connection, Connection)> {
    let bus = tinybus::transport::memory::MemoryBus::new();
    tinybus::broker::Broker::new().spawn(bus.clone());
    let server = Connection::connect(bus.connect().await?).await?;
    setup(server.clone()).await?;
    let client = Connection::connect(bus.connect().await?).await?;
    Ok((server, client))
}

#[tokio::test]
async fn authoring_gate_reports_preserve_prompt_schema_and_language_checks() -> Result<()> {
    let (_server, client) = authoring_connections().await?;
    let proxy = client.proxy(BUS_NAME, OBJECT_PATH, tinyflows_bus::INTERFACE)?;
    let failures: Vec<String> = proxy
        .call(
            "AuthoringFailures",
            (json!({
                "nodes": [
                    {"id":"agent", "name":"Agent", "kind":"agent", "config": {
                        "prompt":"=Look at the diff and fix it",
                        "output_parser":{"schema":{"properties":{}}}
                    }},
                    {"id":"code", "name":"Code", "kind":"code",
                        "config":{"language":"shell"}},
                    {"id":"notify", "name":"Notify", "kind":"tool_call", "config": {
                        "slug":"host:send", "args":{"title":"=nodes.agent.item.json.title"}
                    }}
                ], "edges":[]
            }),),
        )
        .await?;
    assert!(
        failures.iter().any(|failure| failure.contains("prompt")),
        "{failures:?}"
    );
    assert!(
        failures.iter().any(|failure| failure.contains("schema")),
        "{failures:?}"
    );
    assert!(
        failures.iter().any(|failure| failure.contains("language")),
        "{failures:?}"
    );
    Ok(())
}

#[tokio::test]
async fn preflight_checks_respect_host_supplied_native_prefixes() -> Result<()> {
    let (_server, client) = authoring_connections().await?;
    let proxy = client.proxy(BUS_NAME, OBJECT_PATH, tinyflows_bus::INTERFACE)?;
    let graph = json!({"nodes":[
        {"id":"t", "name":"Trigger", "kind":"trigger", "config":{}},
        {"id":"shape", "name":"Shape", "kind":"transform",
            "config":{"expression":"={ body: \"hi\" }"}},
        {"id":"send", "name":"Send", "kind":"tool_call",
            "config":{"slug":"host:send_message", "args":{"subject":"=nodes.shape.item.subject"}}}
    ], "edges":[{"from_node":"t", "to_node":"shape"},
                 {"from_node":"shape", "to_node":"send"}]});
    let failures: Vec<String> = proxy
        .call(
            "UnresolvableToolArgs",
            (graph.clone(), Vec::<String>::new()),
        )
        .await?;
    assert_eq!(failures.len(), 1, "{failures:?}");
    assert!(failures[0].contains("subject"));
    let failures: Vec<String> = proxy
        .call("UnresolvableToolArgs", (graph, vec!["host:".to_owned()]))
        .await?;
    assert!(failures.is_empty(), "{failures:?}");
    Ok(())
}

#[tokio::test]
async fn authoring_checks_reject_unparseable_graphs_with_engine_diagnostics() -> Result<()> {
    let (_server, client) = authoring_connections().await?;
    let proxy = client.proxy(BUS_NAME, OBJECT_PATH, tinyflows_bus::INTERFACE)?;
    let result: Result<Vec<String>> = proxy
        .call("AuthoringFailures", (json!({"nodes":"invalid"}),))
        .await;
    assert!(result.unwrap_err().to_string().contains("invalid type"));
    let result: Result<Vec<String>> = proxy
        .call(
            "UnresolvableToolArgs",
            (json!({"nodes":"invalid"}), Vec::<String>::new()),
        )
        .await;
    assert!(result.unwrap_err().to_string().contains("invalid type"));
    Ok(())
}

struct GateCallback(std::sync::Arc<std::sync::Mutex<Vec<String>>>);

#[tinybus::interface(name = "ai.tinyhumans.tinyflows.AuthoringHost")]
impl GateCallback {
    async fn resolve_workflow(&self, _: String) -> Result<Option<serde_json::Value>> {
        Ok(None)
    }
    async fn check_gate(&self, stage: String, _: serde_json::Value) -> Result<Vec<String>> {
        self.0.lock().unwrap().push(stage.clone());
        Ok(if stage == "connections" {
            vec!["account mismatch".into()]
        } else {
            vec![]
        })
    }
}

#[tokio::test]
async fn builder_gate_module_calls_host_stages_in_order_and_stops_on_failure() -> Result<()> {
    let (_server, client) = authoring_connections().await?;
    let calls = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    client
        .serve_at("/authoring_host".try_into()?, GateCallback(calls.clone()))
        .await?;
    client.request_name("test.AuthoringHost").await?;
    let proxy = client.proxy(BUS_NAME, OBJECT_PATH, tinyflows_bus::INTERFACE)?;
    let failures: Vec<String> = proxy
        .call(
            "BuilderFailures",
            (
                json!({"nodes":[{"id":"start", "name":"Start", "kind":"trigger"}], "edges":[]}),
                json!({"bus_name":"test.AuthoringHost", "object_path":"/authoring_host"}),
                Vec::<String>::new(),
            ),
        )
        .await?;
    assert_eq!(failures, ["account mismatch"]);
    assert_eq!(
        *calls.lock().unwrap(),
        ["tool_policy", "agent_references", "connections"]
    );
    Ok(())
}

#[tokio::test]
async fn builder_gate_callback_transport_failure_is_not_a_successful_report() -> Result<()> {
    let (_server, client) = authoring_connections().await?;
    let proxy = client.proxy(BUS_NAME, OBJECT_PATH, tinyflows_bus::INTERFACE)?;
    let result: Result<Vec<String>> = proxy
        .call(
            "BuilderFailures",
            (
                json!({"nodes":[{"id":"start", "name":"Start", "kind":"trigger"}], "edges":[]}),
                tinyflows_bus::AuthoringHost {
                    bus_name: "test.MissingAuthoringHost".into(),
                    object_path: "/missing".into(),
                },
                Vec::<String>::new(),
            ),
        )
        .await;
    assert!(
        result.is_err(),
        "a missing host must not pass the gate: {result:?}"
    );
    Ok(())
}
