use super::*;

use async_trait::async_trait;
use serde_json::json;
use std::sync::Arc;
use crate::caps::{ToolInvoker, mock};
use crate::error::Result;

struct PendingTool;

#[async_trait]
impl ToolInvoker for PendingTool {
    async fn invoke(&self, _: &str, _: Value, _: Option<&str>) -> Result<Value> {
        std::future::pending().await
    }
}

#[tokio::test]
async fn sandbox_execution_stops_at_its_deadline() {
    let graph = crate::migrate::deserialize_graph(json!({
        "nodes":[
            {"id":"trigger","name":"Trigger","kind":"trigger","config":{}},
            {"id":"invoke","name":"Invoke","kind":"tool_call","config":{"slug":"demo:echo","args":{}}}
        ],
        "edges":[{"from_node":"trigger","to_node":"invoke"}]
    }))
    .unwrap();
    let compiled = crate::compiler::compile(&graph).unwrap();
    let mut capabilities = mock::mock_capabilities();
    capabilities.tools = Arc::new(PendingTool);

    let error = execute(
        &graph,
        &compiled,
        json!({}),
        &capabilities,
        Duration::from_millis(1),
    )
    .await
    .unwrap_err();

    assert!(matches!(error, SandboxError::TimedOut));
}
