use super::{
    HostCallback, authoring_contract_calls, copilot_artifact, portability_artifact,
    wiring_warning_artifact,
};
use serde_json::json;
use tinybus::Connection;
use tinyflows_bus::{BUS_NAME, INTERFACE, OBJECT_PATH, methods};

pub async fn verify(connection: &Connection) {
    let proxy = connection.proxy(BUS_NAME, OBJECT_PATH, INTERFACE).unwrap();
    let catalog_proxy = connection
        .proxy(
            BUS_NAME,
            tinyflows_bus::catalog::OBJECT_PATH,
            tinyflows_bus::catalog::INTERFACE,
        )
        .unwrap();
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

    let graph = authoring_contract_calls::verify_surface(&proxy, &catalog_proxy).await;
    let manifest = authoring_contract_calls::approval_manifest(connection, &catalog_proxy).await;
    assert_eq!(manifest.len(), 1);
    assert_eq!(manifest[0]["node_id"], "code");
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

    wiring_warning_artifact::verify_surface(&proxy).await;
    portability_artifact::verify_surface(connection).await;
    wiring_warning_artifact::verify_authoring_gates(&proxy).await;
    copilot_artifact::verify_surface(connection).await;
}
