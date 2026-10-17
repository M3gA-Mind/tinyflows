//! First proposal tool: validate a candidate graph without persisting it.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use tinyflows::authoring::validate_and_migrate_graph;
use tinytools::{PermissionLevel, Tool, ToolResult};

use super::ports::{BuilderGates, NodeKindSource};
use super::static_schema;

/// Validates a new workflow candidate and returns its review proposal.
pub struct ProposeWorkflowTool {
    gates: Arc<dyn BuilderGates>,
    description: String,
}

impl ProposeWorkflowTool {
    /// Creates a proposal tool over the caller's scoped gates.
    pub fn new(gates: Arc<dyn BuilderGates>, kinds: Arc<dyn NodeKindSource>) -> Self {
        let required = tinyflows::catalog::render_node_kinds_required(&kinds.all());
        let description = format!(
            "Propose a candidate automation workflow for the user to review and save. This tool ONLY VALIDATES the graph and returns a summary — it NEVER creates or enables the flow; the user must click \"Save & enable\" in the UI before anything is persisted or can run. If validation fails, fix the graph and call this tool again.\n\nBuild a tinyflows WorkflowGraph: nodes[] ({{id, kind, name, config}}) + edges[] ({{from_node, to_node, from_port?, to_port?}}; ports default \"main\"). Exactly ONE trigger node is required.\n\nBranching (condition/switch): the branch label goes on from_port, NEVER on to_port (which stays \"main\"). Routing is keyed exclusively on from_port, so a label on to_port silently turns the branch into an unconditional fan-out and is a hard reject.\n\nA memory node may only target config.scope \"flow\" for remember/forget; \"user\" is READ-ONLY and a write to it is a hard reject.\n\nNode kinds and their required config: {required}. Call `get_node_kind_contract {{ kind }}` for a kind's optional fields, ports, a worked example, and its gotchas — it is generated from the catalog the validator enforces, so it is always current."
        );
        Self { gates, description }
    }
}

#[async_trait]
impl Tool for ProposeWorkflowTool {
    fn name(&self) -> &str {
        "propose_workflow"
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn parameters_schema(&self) -> Value {
        static_schema!(include_str!("parameters/propose_workflow.json"))
    }

    fn permission_level(&self) -> PermissionLevel {
        PermissionLevel::None
    }

    fn external_effect(&self) -> bool {
        false
    }

    async fn execute(&self, args: Value) -> anyhow::Result<ToolResult> {
        let name = match args.get("name").and_then(Value::as_str).map(str::trim) {
            Some(name) if !name.is_empty() => name.to_owned(),
            _ => return Ok(ToolResult::error("Missing 'name' parameter")),
        };
        let graph = match args.get("graph") {
            Some(graph) if !graph.is_null() => graph.clone(),
            _ => return Ok(ToolResult::error("Missing 'graph' parameter")),
        };
        let require_approval = args
            .get("require_approval")
            .and_then(Value::as_bool)
            .unwrap_or(true);
        let graph = match validate_and_migrate_graph(graph) {
            Ok(graph) => graph,
            Err(error) => {
                return Ok(ToolResult::error(format!(
                    "Workflow graph is invalid: {error}. Fix the graph and call propose_workflow again."
                )));
            }
        };
        match self
            .gates
            .build_proposal(
                "propose_workflow",
                &name,
                &graph,
                require_approval,
                false,
                None,
                None,
                None,
            )
            .await
        {
            Ok(payload) => Ok(ToolResult::success(serde_json::to_string_pretty(&payload)?)),
            Err(message) => Ok(ToolResult::error(message)),
        }
    }
}

#[cfg(test)]
#[path = "propose_tests.rs"]
mod tests;
