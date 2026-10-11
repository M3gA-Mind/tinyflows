use serde_json::json;
use std::sync::Arc;
use tinybus::Connection;
use tinybus::broker::Broker;
use tinybus::module::ModuleHost;
use tinybus::transport::memory::MemoryTransport;
use tinyflows_bus::{BUS_NAME, GraphValidation, INTERFACE, METHODS, OBJECT_PATH, methods};

struct HostCallback;

#[tinybus::interface(name = "ai.tinyhumans.tinyflows.AuthoringHost")]
impl HostCallback {
    async fn resolve_workflow(&self, _: String) -> tinybus::Result<Option<serde_json::Value>> {
        Ok(None)
    }
    async fn check_gate(
        &self,
        stage: String,
        _: serde_json::Value,
    ) -> tinybus::Result<Vec<String>> {
        Ok(if stage == "connections" {
            vec!["artifact callback refused account".into()]
        } else {
            vec![]
        })
    }
}

#[tokio::test]
#[ignore = "run through scripts/verify-module-artifact.sh"]
async fn compiled_module_serves_validation_without_linking_engine_into_client() {
    let artifact = std::env::var_os("TINYFLOWS_MODULE_ARTIFACT")
        .expect("set TINYFLOWS_MODULE_ARTIFACT to the built cdylib");
    let broker = Broker::new();
    let host = ModuleHost::new(broker.clone());
    let info = host.load_file(std::path::PathBuf::from(artifact)).unwrap();
    assert!(
        info.manifest
            .requires
            .iter()
            .any(|dependency| dependency.optional
                && dependency.interface.interface.as_str() == tinyflows_bus::HOST_INTERFACE)
    );
    let provided = info
        .manifest
        .provides
        .iter()
        .find(|provided| provided.version.interface.as_str() == INTERFACE)
        .unwrap();
    let mut actual: Vec<_> = provided.methods.iter().map(ToString::to_string).collect();
    let mut expected: Vec<_> = METHODS.iter().map(|member| member.to_string()).collect();
    actual.sort();
    expected.sort();
    assert_eq!(actual, expected);

    let (client, transport) = MemoryTransport::pair();
    broker.attach(Arc::new(transport));
    let connection = Connection::connect(Box::new(client)).await.unwrap();
    let proxy = connection.proxy(BUS_NAME, OBJECT_PATH, INTERFACE).unwrap();
    connection
        .serve_at("/authoring_host".try_into().unwrap(), HostCallback)
        .await
        .unwrap();
    connection
        .request_name("test.ArtifactAuthoringHost")
        .await
        .unwrap();
    let failures: Vec<String> = proxy
        .call(
            methods::BUILDER_FAILURES,
            (
                json!({"nodes":[{"id":"start", "name":"Start", "kind":"trigger"}], "edges":[]}),
                tinyflows_bus::AuthoringHost {
                    bus_name: "test.ArtifactAuthoringHost".into(),
                    object_path: "/authoring_host".into(),
                },
                Vec::<String>::new(),
            ),
        )
        .await
        .unwrap();
    assert_eq!(failures, ["artifact callback refused account"]);

    let report: GraphValidation = proxy
        .call(
            methods::VALIDATE_GRAPH,
            (json!({
                "nodes": [{"id":"start", "name":"Start", "kind":"trigger", "config":{}}],
                "edges": []
            }),),
        )
        .await
        .unwrap();
    assert!(report.is_valid(), "{:?}", report.errors);
    let graph = report.graph.unwrap();
    assert!(graph["schema_version"].as_u64().unwrap() > 0);
    let failures: Vec<String> = proxy
        .call(methods::BINDING_FAILURES, (graph,))
        .await
        .unwrap();
    assert_eq!(failures, Vec::<String>::new());

    let failures: Vec<String> = proxy
        .call(
            methods::BINDING_FAILURES,
            (json!({
                "nodes": [
                    {"id":"fetch", "name":"Fetch", "kind":"agent",
                     "config":{"prompt":"Fetch the title"}},
                    {"id":"notify", "name":"Notify", "kind":"tool_call",
                     "config":{"slug":"demo:echo",
                               "args":{"text":"=nodes.fetch.item.title"}}},
                    {"id":"code", "name":"Code", "kind":"code",
                     "config":{"language":"shell"}}
                ],
                "edges": []
            }),),
        )
        .await
        .unwrap();
    assert_eq!(failures.len(), 1, "{failures:?}");
    assert!(failures[0].contains("=nodes.fetch.item.json.title"));

    let failures: Vec<String> = proxy
        .call(
            methods::AUTHORING_FAILURES,
            (json!({
                "nodes":[{"id":"code", "name":"Code", "kind":"code",
                          "config":{"language":"shell"}}], "edges":[]
            }),),
        )
        .await
        .unwrap();
    assert_eq!(failures.len(), 1, "{failures:?}");
    assert!(failures[0].contains("language"));

    let graph = json!({"nodes":[
        {"id":"t", "name":"Trigger", "kind":"trigger", "config":{}},
        {"id":"shape", "name":"Shape", "kind":"transform",
         "config":{"expression":"={ body: \"hi\" }"}},
        {"id":"send", "name":"Send", "kind":"tool_call",
         "config":{"slug":"host:send", "args":{"subject":"=nodes.shape.item.subject"}}}
    ], "edges":[{"from_node":"t", "to_node":"shape"},
                 {"from_node":"shape", "to_node":"send"}]});
    let failures: Vec<String> = proxy
        .call(
            methods::UNRESOLVABLE_TOOL_ARGS,
            (graph.clone(), Vec::<String>::new()),
        )
        .await
        .unwrap();
    assert_eq!(failures.len(), 1, "{failures:?}");
    assert!(failures[0].contains("subject"));
    let failures: Vec<String> = proxy
        .call(
            methods::UNRESOLVABLE_TOOL_ARGS,
            (graph, vec!["host:".to_owned()]),
        )
        .await
        .unwrap();
    assert!(failures.is_empty(), "{failures:?}");
}
