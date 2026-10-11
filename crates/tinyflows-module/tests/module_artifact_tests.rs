#[path = "support/schedule_artifact_callbacks.rs"]
mod callbacks;
use serde_json::json;
use std::sync::Arc;
use tinybus::Connection;
use tinybus::broker::Broker;
use tinybus::module::ModuleHost;
use tinybus::transport::memory::MemoryTransport;
use tinyflows_bus::{BUS_NAME, GraphValidation, INTERFACE, METHODS, OBJECT_PATH, methods};

#[tokio::test]
#[ignore = "run through scripts/verify-module-artifact.sh"]
async fn compiled_module_serves_validation_without_linking_engine_into_client() {
    let artifact = std::env::var_os("TINYFLOWS_MODULE_ARTIFACT")
        .expect("set TINYFLOWS_MODULE_ARTIFACT to the built cdylib");
    let broker = Broker::new();
    let host = ModuleHost::new(broker.clone());
    let info = host.load_file(std::path::PathBuf::from(artifact)).unwrap();
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

    let schedule_interface = info
        .manifest
        .provides
        .iter()
        .find(|provided| provided.version.interface.as_str() == tinyflows_bus::schedule::INTERFACE)
        .unwrap();
    let mut actual: Vec<_> = schedule_interface
        .methods
        .iter()
        .map(ToString::to_string)
        .collect();
    let mut expected: Vec<_> = tinyflows_bus::schedule::METHODS
        .iter()
        .map(|s| s.to_string())
        .collect();
    actual.sort();
    expected.sort();
    assert_eq!(actual, expected);

    let (client, transport) = MemoryTransport::pair();
    broker.attach(Arc::new(transport));
    let connection = Connection::connect(Box::new(client)).await.unwrap();
    let schedule_proxy = connection
        .proxy(
            tinyflows_bus::BUS_NAME,
            tinyflows_bus::schedule::OBJECT_PATH,
            tinyflows_bus::schedule::INTERFACE,
        )
        .unwrap();
    let delay: i64 = schedule_proxy
        .call("ParseHumanDelay", ("2h",))
        .await
        .unwrap();
    assert_eq!(delay, 7_200_000);

    let state = Arc::new(callbacks::State::default());
    connection
        .serve_at(
            "/test/ScheduleCallbacks".try_into().unwrap(),
            callbacks::Callbacks(state.clone()),
        )
        .await
        .unwrap();
    connection
        .request_name("test.ScheduleCallbacks")
        .await
        .unwrap();
    let request = tinyflows_bus::schedule::StartScheduler {
        scope: "artifact-scope".into(),
        callback: tinyflows_bus::schedule::CallbackAddress {
            destination: "test.ScheduleCallbacks".into(),
            object_path: "/test/ScheduleCallbacks".into(),
        },
        config: json!({}),
        manual_ticks: true,
    };
    let handle: String = schedule_proxy
        .call("StartScheduler", (request,))
        .await
        .unwrap();
    let count: usize = schedule_proxy
        .call("TickScheduler", (&handle,))
        .await
        .unwrap();
    assert_eq!(count, 1);
    schedule_proxy
        .call::<()>("DrainScheduler", (&handle,))
        .await
        .unwrap();
    schedule_proxy
        .call::<()>("StopScheduler", (&handle,))
        .await
        .unwrap();
    assert_eq!(*state.triggers.lock().unwrap(), ["artifact-flow"]);
    assert_eq!(state.records.lock().unwrap()[0]["status"], "ok");
    assert_eq!(state.completions.lock().unwrap()[0]["disable"], true);

    let proxy = connection.proxy(BUS_NAME, OBJECT_PATH, INTERFACE).unwrap();
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
}
