use serde_json::json;
use std::sync::Arc;
use tinybus::Connection;
use tinybus::broker::Broker;
use tinybus::module::ModuleHost;
use tinybus::transport::memory::MemoryTransport;

#[tokio::test]
#[ignore = "run with TINYFLOWS_MODULE_ARTIFACT set to a compiled module"]
async fn compiled_module_prepares_manual_worker_through_schedule_abi() {
    let artifact = std::env::var_os("TINYFLOWS_MODULE_ARTIFACT")
        .expect("set TINYFLOWS_MODULE_ARTIFACT to a compiled cdylib");
    let broker = Broker::new();
    let host = ModuleHost::new(broker.clone());
    host.load_file(std::path::PathBuf::from(artifact)).unwrap();
    let (client, transport) = MemoryTransport::pair();
    broker.attach(Arc::new(transport));
    let connection = Connection::connect(Box::new(client)).await.unwrap();
    let schedule = connection
        .proxy(
            tinyflows_bus::schedule::BUS_NAME,
            tinyflows_bus::schedule::OBJECT_PATH,
            tinyflows_bus::schedule::INTERFACE,
        )
        .unwrap();

    let prepared: serde_json::Value = schedule
        .call(
            "PrepareManualRun",
            (json!({"scope":"artifact-scope","request_id":"artifact-request"}),),
        )
        .await
        .unwrap();
    assert!(prepared["token"].is_string(), "prepare returns an opaque handle");
    let handle = prepared;
    let stopped: serde_json::Value = schedule
        .call("StopManualRun", (json!({"handle":handle}),))
        .await
        .unwrap();
    assert_eq!(stopped["kind"], "terminal");
    let acknowledged: bool = schedule
        .call("AcknowledgeManualRun", (json!({"handle":handle}),))
        .await
        .unwrap();
    assert!(acknowledged);

    let late_start = schedule
        .call::<serde_json::Value>(
            "StartManualRun",
            (json!({
                "handle":handle.clone(),
                "request":{
                    "callback":{"destination":"test.Callbacks","object_path":"/test/Callbacks"},
                    "config":{},
                    "storage":{"backend":{"kind":"documents","callback":{"destination":"test.Callbacks","object_path":"/test/Documents"}}},
                    "job":{
                        "id":"stale-job","expression":"","schedule":{"kind":"at","at":"2026-01-01T00:00:00Z"},
                        "command":"echo stale","prompt":null,"name":null,"job_type":"shell","session_target":"isolated",
                        "model":null,"agent_id":null,"enabled":true,"delivery":{"mode":"none"},"delete_after_run":false,
                        "created_at":"2026-01-01T00:00:00Z","next_run":"2026-01-01T00:00:00Z"
                    },
                    "run_id":"stale-run","delivery":"not_requested"
                }
            }),),
        )
        .await;
    assert!(late_start.is_err(), "an acknowledged handle cannot start later");
    let prepared_again: serde_json::Value = schedule
        .call(
            "PrepareManualRun",
            (json!({"scope":"artifact-scope","request_id":"artifact-request"}),),
        )
        .await
        .unwrap();
    assert_ne!(
        prepared_again["token"], handle["token"],
        "reusing a request id after acknowledgement mints a new opaque handle"
    );
}
