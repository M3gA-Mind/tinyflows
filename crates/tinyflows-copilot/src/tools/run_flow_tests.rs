use super::*;
use serde_json::json;
use tinytools::Tool;

#[tokio::test]
async fn run_flow_starts_detached_and_forwards_declared_inputs() {
    let host = Arc::new(crate::tools::fakes::MemHost::default());
    let tool = RunFlowTool::new(host.clone());
    assert_eq!(tool.name(), "run_flow");
    assert_eq!(tool.permission_level(), PermissionLevel::Write);
    assert!(tool.external_effect());
    let result = tool
        .execute(json!({
            "flow_id":"flow-7",
            "input":{"event":"test"},
            "inputs":{"repo":"tinyflows"}
        }))
        .await
        .unwrap();
    assert!(!result.is_error);
    let response: Value = serde_json::from_str(&result.text()).unwrap();
    assert_eq!(response["flow_id"], "flow-7");
    assert_eq!(response["status"], "running");
    assert_eq!(response["result"]["input"]["event"], "test");
    assert_eq!(response["result"]["inputs"]["repo"], "tinyflows");
    assert_eq!(*host.calls.lock().unwrap(), ["run flow-7"]);
}
