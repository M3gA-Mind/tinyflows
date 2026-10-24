//! Request-scoped adapters that let the module-owned tools call host policy.

use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use tinybus::Connection;
use tinyflows::model::WorkflowGraph;
use tinyflows_catalog::types::{
    DraftOrigin, Flow, FlowDraft, FlowRevision, FlowRun, FlowSuggestion, FlowValidation,
};
use tinyflows_copilot::tools::ports::{
    AgentDefinition, AgentDirectory, BuilderGates, DraftStore, FlowStore, PortResult,
    SandboxRunRequest, SandboxRunner, SuggestionStore, Written,
};

#[derive(Clone)]
pub(super) struct ScopedCopilotHost {
    pub connection: Connection,
    pub host: tinyflows_bus::copilot::CopilotHost,
}

impl ScopedCopilotHost {
    async fn call(
        &self,
        operation: tinyflows_bus::copilot::PortOperation,
        payload: Value,
    ) -> PortResult<Value> {
        let proxy = self
            .connection
            .proxy(
                &self.host.bus_name,
                &self.host.object_path,
                tinyflows_bus::copilot::HOST_INTERFACE,
            )
            .map_err(|error| error.to_string())?;
        proxy
            .call(
                tinyflows_bus::copilot::host::CALL_PORT,
                (tinyflows_bus::copilot::PortCallRequest { operation, payload },),
            )
            .await
            .map_err(|error| error.to_string())
    }

    async fn call_as<T: DeserializeOwned>(
        &self,
        operation: tinyflows_bus::copilot::PortOperation,
        payload: Value,
    ) -> PortResult<T> {
        serde_json::from_value(self.call(operation, payload).await?)
            .map_err(|error| error.to_string())
    }

    async fn build_proposal(
        &self,
        request: tinyflows_bus::copilot::BuildProposalRequest,
    ) -> PortResult<Value> {
        let proxy = self
            .connection
            .proxy(
                &self.host.bus_name,
                &self.host.object_path,
                tinyflows_bus::copilot::HOST_INTERFACE,
            )
            .map_err(|error| error.to_string())?;
        proxy
            .call(tinyflows_bus::copilot::host::BUILD_PROPOSAL, (request,))
            .await
            .map_err(|error| error.to_string())
    }

    async fn run_flow(&self, request: tinyflows_bus::copilot::RunFlowRequest) -> PortResult<Value> {
        let proxy = self
            .connection
            .proxy(
                &self.host.bus_name,
                &self.host.object_path,
                tinyflows_bus::copilot::HOST_INTERFACE,
            )
            .map_err(|error| error.to_string())?;
        proxy
            .call(tinyflows_bus::copilot::host::RUN_FLOW, (request,))
            .await
            .map_err(|error| error.to_string())
    }
}

fn decode_library_type<T: DeserializeOwned>(wire: impl Serialize) -> PortResult<T> {
    serde_json::from_value(serde_json::to_value(wire).map_err(|error| error.to_string())?)
        .map_err(|error| error.to_string())
}

#[async_trait]
impl SandboxRunner for ScopedCopilotHost {
    async fn run(&self, request: SandboxRunRequest) -> PortResult<Value> {
        if let Some(callback_error) = request.permissions_manifest.iter().find_map(|entry| {
            entry
                .get("__copilot_callback_error")
                .and_then(Value::as_str)
        }) {
            return Err(format!(
                "could not classify sandbox permissions: {callback_error}"
            ));
        }
        let proxy = self
            .connection
            .proxy(
                tinyflows_bus::BUS_NAME,
                tinyflows_bus::runs::OBJECT_PATH,
                tinyflows_bus::runs::INTERFACE,
            )
            .map_err(|error| error.to_string())?;
        let report: tinyflows_bus::runs::SandboxRunResponse = proxy
            .call(
                tinyflows_bus::runs::methods::SANDBOX_RUN,
                (tinyflows_bus::runs::SandboxRun {
                    graph: request.graph,
                    input: request.input,
                    tool_invoker: tinyflows_bus::runs::CallbackAddress {
                        destination: self.host.bus_name.clone(),
                        object_path: self.host.object_path.clone(),
                    },
                    permissions_manifest: request.permissions_manifest,
                },),
            )
            .await
            .map_err(|error| error.to_string())?;
        Ok(report.result)
    }
}

#[async_trait]
impl BuilderGates for ScopedCopilotHost {
    async fn validate(&self, graph: Value) -> FlowValidation {
        let response = self
            .call_as::<tinyflows_bus::copilot::FlowValidation>(
                tinyflows_bus::copilot::port_operations::GATES_VALIDATE,
                graph,
            )
            .await;
        match response.and_then(decode_library_type) {
            Ok(report) => report,
            Err(error) => FlowValidation {
                valid: false,
                errors: vec![format!("could not validate workflow: {error}")],
                ..FlowValidation::default()
            },
        }
    }

    async fn run_gates(&self, graph: &WorkflowGraph) -> Vec<String> {
        self.call_as(
            tinyflows_bus::copilot::port_operations::GATES_RUN,
            serde_json::to_value(graph).unwrap_or(Value::Null),
        )
        .await
        .unwrap_or_else(|error| vec![error])
    }

    async fn strict_gate(&self, graph: &Value) -> PortResult<()> {
        let allowed: bool = self
            .call_as(
                tinyflows_bus::copilot::port_operations::GATES_STRICT,
                graph.clone(),
            )
            .await?;
        if allowed {
            Ok(())
        } else {
            Err("host strict gate rejected the workflow".into())
        }
    }

    async fn engine_compatibility_errors(&self, graph: &WorkflowGraph) -> Vec<String> {
        self.call_as(
            tinyflows_bus::copilot::port_operations::GATES_ENGINE_COMPATIBILITY,
            serde_json::to_value(graph).unwrap_or(Value::Null),
        )
        .await
        .unwrap_or_else(|error| vec![error])
    }

    async fn wiring_warnings(&self, graph: &WorkflowGraph) -> Vec<String> {
        self.call_as(
            tinyflows_bus::copilot::port_operations::GATES_WIRING_WARNINGS,
            serde_json::to_value(graph).unwrap_or(Value::Null),
        )
        .await
        .unwrap_or_default()
    }

    async fn approval_manifest(&self, graph: &WorkflowGraph) -> Vec<Value> {
        self.call_as::<Vec<Value>>(
            tinyflows_bus::copilot::port_operations::GATES_APPROVAL_MANIFEST,
            serde_json::to_value(graph).unwrap_or(Value::Null),
        )
        .await
        .unwrap_or_else(|error| vec![json!({"__copilot_callback_error":error})])
    }

    async fn build_proposal(
        &self,
        retry_tool: &str,
        name: &str,
        graph: &WorkflowGraph,
        require_approval: bool,
        revision: bool,
        instruction: Option<String>,
        draft_id: Option<String>,
        flow_id: Option<String>,
    ) -> Result<Value, String> {
        let graph = serde_json::to_value(graph).map_err(|error| error.to_string())?;
        self.build_proposal(tinyflows_bus::copilot::BuildProposalRequest {
            retry_tool: retry_tool.into(),
            name: name.into(),
            graph,
            require_approval,
            revision,
            instruction,
            draft_id,
            flow_id,
        })
        .await
    }
}

#[async_trait]
impl DraftStore for ScopedCopilotHost {
    async fn get(&self, id: &str) -> PortResult<FlowDraft> {
        let draft = self
            .call_as::<tinyflows_bus::catalog::FlowDraft>(
                tinyflows_bus::copilot::port_operations::DRAFT_GET,
                json!({"id": id}),
            )
            .await?;
        decode_library_type(draft)
    }

    async fn create(
        &self,
        flow_id: Option<String>,
        name: String,
        graph: Value,
        origin: DraftOrigin,
    ) -> PortResult<FlowDraft> {
        let draft = self
            .call_as::<tinyflows_bus::catalog::FlowDraft>(
                tinyflows_bus::copilot::port_operations::DRAFT_CREATE,
                json!({"flow_id": flow_id, "name": name, "graph": graph, "origin": origin}),
            )
            .await?;
        decode_library_type(draft)
    }

    async fn update(
        &self,
        id: &str,
        name: Option<String>,
        graph: Option<Value>,
    ) -> PortResult<FlowDraft> {
        let draft = self
            .call_as::<tinyflows_bus::catalog::FlowDraft>(
                tinyflows_bus::copilot::port_operations::DRAFT_UPDATE,
                json!({"id": id, "name": name, "graph": graph}),
            )
            .await?;
        decode_library_type(draft)
    }
}

#[async_trait]
impl AgentDirectory for ScopedCopilotHost {
    async fn list(&self) -> PortResult<Vec<AgentDefinition>> {
        let agents = self
            .call_as::<Vec<tinyflows_bus::copilot::AgentDefinition>>(
                tinyflows_bus::copilot::port_operations::AGENTS_LIST,
                Value::Null,
            )
            .await?;
        decode_library_type(agents)
    }
}

#[async_trait]
impl SuggestionStore for ScopedCopilotHost {
    async fn upsert(&self, suggestions: &[FlowSuggestion]) -> PortResult<usize> {
        let suggestions: Vec<tinyflows_bus::catalog::FlowSuggestion> =
            decode_library_type(suggestions)?;
        self.call_as(
            tinyflows_bus::copilot::port_operations::SUGGESTIONS_UPSERT,
            serde_json::to_value(suggestions).map_err(|error| error.to_string())?,
        )
        .await
    }
}

#[async_trait]
impl FlowStore for ScopedCopilotHost {
    async fn get(&self, _id: &str) -> PortResult<Flow> {
        let flow = self
            .call_as::<tinyflows_bus::catalog::Workflow>(
                tinyflows_bus::copilot::port_operations::FLOWS_GET,
                json!({"id": _id}),
            )
            .await?;
        decode_library_type(flow)
    }
    async fn list(&self) -> PortResult<Vec<Flow>> {
        let flows = self
            .call_as::<Vec<tinyflows_bus::catalog::Workflow>>(
                tinyflows_bus::copilot::port_operations::FLOWS_LIST,
                Value::Null,
            )
            .await?;
        decode_library_type(flows)
    }
    async fn load_graph(&self, _id: &str) -> PortResult<Option<WorkflowGraph>> {
        self.call_as(
            tinyflows_bus::copilot::port_operations::FLOWS_LOAD_GRAPH,
            json!({"id": _id}),
        )
        .await
    }
    async fn create(&self, _name: String, _graph: Value, _approval: bool) -> PortResult<Flow> {
        let flow = self
            .call_as::<tinyflows_bus::catalog::Workflow>(
                tinyflows_bus::copilot::port_operations::FLOWS_CREATE,
                json!({"name": _name, "graph": _graph, "require_approval": _approval}),
            )
            .await?;
        decode_library_type(flow)
    }
    async fn update(
        &self,
        _id: &str,
        _name: Option<String>,
        _graph: Option<Value>,
    ) -> PortResult<Written<Flow>> {
        let flow = self
            .call_as::<tinyflows_bus::copilot::WrittenWorkflow>(
                tinyflows_bus::copilot::port_operations::FLOWS_UPDATE,
                json!({"id": _id, "name": _name, "graph": _graph}),
            )
            .await?;
        decode_library_type(flow)
    }
    async fn set_enabled(&self, _id: &str, _enabled: bool) -> PortResult<Flow> {
        let flow = self
            .call_as::<tinyflows_bus::catalog::Workflow>(
                tinyflows_bus::copilot::port_operations::FLOWS_SET_ENABLED,
                json!({"id": _id, "enabled": _enabled}),
            )
            .await?;
        decode_library_type(flow)
    }
    async fn duplicate(&self, _id: &str) -> PortResult<Flow> {
        let flow = self
            .call_as::<tinyflows_bus::catalog::Workflow>(
                tinyflows_bus::copilot::port_operations::FLOWS_DUPLICATE,
                json!({"id": _id}),
            )
            .await?;
        decode_library_type(flow)
    }
    async fn history(&self, _id: &str, _limit: usize) -> PortResult<Vec<FlowRevision>> {
        let revisions = self
            .call_as::<Vec<tinyflows_bus::catalog::WorkflowRevision>>(
                tinyflows_bus::copilot::port_operations::FLOWS_HISTORY,
                json!({"id": _id, "limit": _limit}),
            )
            .await?;
        decode_library_type(revisions)
    }
    async fn list_runs(&self, _id: &str, _limit: usize) -> PortResult<Vec<FlowRun>> {
        let runs = self
            .call_as::<Vec<tinyflows_bus::catalog::FlowRun>>(
                tinyflows_bus::copilot::port_operations::FLOWS_LIST_RUNS,
                json!({"flow_id": _id, "limit": _limit}),
            )
            .await?;
        decode_library_type(runs)
    }
    async fn get_run(&self, _id: &str) -> PortResult<FlowRun> {
        let run = self
            .call_as::<tinyflows_bus::catalog::FlowRun>(
                tinyflows_bus::copilot::port_operations::FLOWS_GET_RUN,
                json!({"run_id": _id}),
            )
            .await?;
        decode_library_type(run)
    }
    async fn resume(
        &self,
        _flow: &str,
        _run: &str,
        _approve: Vec<String>,
        _reject: Vec<String>,
    ) -> PortResult<Value> {
        self.call_as(
            tinyflows_bus::copilot::port_operations::FLOWS_RESUME,
            json!({"flow_id": _flow, "run_id": _run, "approve": _approve, "reject": _reject}),
        )
        .await
    }
    async fn cancel_run(&self, _id: &str) -> PortResult<Value> {
        self.call_as(
            tinyflows_bus::copilot::port_operations::FLOWS_CANCEL_RUN,
            json!({"run_id": _id}),
        )
        .await
    }
    async fn run_detached(
        &self,
        flow_id: &str,
        input: Value,
        inputs: serde_json::Map<String, Value>,
    ) -> PortResult<Value> {
        self.run_flow(tinyflows_bus::copilot::RunFlowRequest {
            flow_id: flow_id.into(),
            input,
            inputs,
        })
        .await
    }
}

#[cfg(test)]
#[path = "copilot_wire_compat_tests.rs"]
mod tests;
