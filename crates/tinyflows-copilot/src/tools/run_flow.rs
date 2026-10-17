//! Start a saved workflow as a detached real run.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Value, json};
use tinytools::{PermissionLevel, Tool, ToolResult};

use super::ports::FlowStore;

/// Starts a saved workflow and returns immediately with its run id.
pub struct RunFlowTool {
    flows: Arc<dyn FlowStore>,
}

impl RunFlowTool {
    /// Creates the tool over the caller's scoped flow store.
    pub fn new(flows: Arc<dyn FlowStore>) -> Self {
        Self { flows }
    }
}

#[async_trait]
impl Tool for RunFlowTool {
    fn name(&self) -> &str {
        "run_flow"
    }

    fn description(&self) -> &str {
        "Run a SAVED workflow by id to TEST it end-to-end. This is a REAL run, not a simulation — real effects can fire (use dry_run_workflow for a safe MOCK run instead). It only works on a flow the user has already saved; pass its `flow_id`. You MUST ask the user to confirm and wait for an explicit 'yes' before calling this — never run a workflow unprompted. The flow's own approval gate still pauses outbound-action nodes. If the flow declares workflow inputs (read `graph.inputs` via get_flow), pass their values in `inputs` — ask the user for any required one rather than inventing it; a missing or wrongly-typed value is rejected and nothing runs. Params: { flow_id (required), input?, inputs? }. Returns the run's status + any nodes paused for approval."
    }

    fn parameters_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "flow_id": {"type": "string", "description": "Id of the SAVED flow to run (the user must have saved it first)."},
                "input": {"description": "Optional trigger input passed to the run (defaults to {})."},
                "inputs": {"type": "object", "description": "Values for the flow's DECLARED workflow inputs, keyed by name. Read the declarations from the flow's graph.inputs first; ask the user for any required value instead of guessing. Distinct from 'input', the free-form trigger payload."}
            },
            "required": ["flow_id"]
        })
    }

    fn permission_level(&self) -> PermissionLevel {
        PermissionLevel::Write
    }

    fn external_effect(&self) -> bool {
        true
    }

    async fn execute(&self, args: Value) -> anyhow::Result<ToolResult> {
        let flow_id = match args.get("flow_id").and_then(Value::as_str).map(str::trim) {
            Some(id) if !id.is_empty() => id.to_owned(),
            _ => {
                return Ok(ToolResult::error(
                    "Missing 'flow_id' — run_flow only works on a SAVED flow. Ask the user to Save the workflow first, then run it by id.",
                ));
            }
        };
        let input = args.get("input").cloned().unwrap_or_else(|| json!({}));
        let inputs = match args.get("inputs") {
            None | Some(Value::Null) => serde_json::Map::new(),
            Some(Value::Object(inputs)) => inputs.clone(),
            Some(_) => {
                return Ok(ToolResult::error(
                    "'inputs' must be an object keyed by the flow's declared input names, e.g. {\"repo\": \"acme/api\"}. Read the declarations from the flow's graph.inputs.",
                ));
            }
        };
        match self.flows.run_detached(&flow_id, input, inputs).await {
            Ok(result) => {
                let run_id = result
                    .get("run_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                Ok(ToolResult::success(serde_json::to_string_pretty(&json!({
                    "type": "workflow_run_started",
                    "flow_id": flow_id,
                    "run_id": run_id,
                    "status": "running",
                    "detached": true,
                    "note": "The run started in the background and is now 'running'. Poll get_flow_run with this run_id to see it settle to a terminal status (completed / failed / interrupted / pending_approval); do not assume success from this response alone.",
                    "result": result
                }))?))
            }
            Err(error) => Ok(ToolResult::error(format!(
                "Could not start flow '{flow_id}': {error}"
            ))),
        }
    }
}

#[cfg(test)]
#[path = "run_flow_tests.rs"]
mod tests;
