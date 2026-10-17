use super::*;

use serde_json::json;
use std::sync::Arc;
use tinytools::Tool;

use crate::tools::fakes::MemHost;

#[tokio::test]
async fn dry_run_delegates_to_its_sandbox_port_and_preserves_host_manifest() {
    let host = Arc::new(MemHost::default());
    let manifest = vec![json!({"node_id":"send","permission":"write"})];
    *host.sandbox_manifest.lock().unwrap() = manifest.clone();
    let tool = DryRunWorkflowTool::new(host.clone(), host.clone(), host.clone(), host.clone());
    let result = tool
        .execute(json!({
            "graph": {
                "nodes":[
                    {"id":"trigger","name":"Trigger","kind":"trigger","config":{}},
                    {"id":"agent","name":"Agent","kind":"agent","config":{"prompt":"hello"}}
                ],
                "edges":[{"from_node":"trigger","to_node":"agent"}]
            }
        }))
        .await
        .unwrap();

    let report: serde_json::Value = serde_json::from_str(&result.output().to_string()).unwrap();
    assert_eq!(report["sandbox"], true);
    assert_eq!(report["permissions_manifest"], json!(manifest));
    let requests = host.sandbox_requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].permissions_manifest, manifest);
}
