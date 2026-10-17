//! Exercises the scope-bound wiring lookup through the loaded module artifact.

use serde_json::json;
use tinybus::Proxy;
use tinyflows_bus::{AuthoringHost, WiringWarningsRequest, methods};

pub async fn verify_surface(proxy: &Proxy) {
    let warnings: Vec<String> = proxy
        .call(
            methods::WIRING_WARNINGS,
            (WiringWarningsRequest {
                graph: json!({"nodes":[
                    {"id":"tool","name":"Tool","kind":"tool_call","config":{"slug":"provider:issues"}},
                    {"id":"next","name":"Next","kind":"transform","config":{"value":"=nodes.tool.item.json.data.missing"}}
                ],"edges":[]}),
                host: AuthoringHost {
                    bus_name: "test.ArtifactAuthoringHost".into(),
                    object_path: "/authoring_host".into(),
                },
            },),
        )
        .await
        .unwrap();
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert!(warnings[0].contains("missing"));
    assert!(warnings[0].contains("issues"));
}

pub async fn verify_authoring_gates(proxy: &Proxy) {
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
         "config":{"set":{"subject":{}}}},
        {"id":"send", "name":"Send", "kind":"tool_call",
         "config":{"slug":"host:send", "args":{"subject":"=nodes.shape.item.subject.missing"}}}
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
