//! TinyBus entry points for module-owned workflow run lifecycle.

use tinybus::{Connection, Error, Result};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Value, json};
use tinyflows::caps::{self, ToolInvoker};
use tinyflows::error::{EngineError, Result as EngineResult};
use tinyflows::model::WorkflowGraph;
use tinyflows_bus::runs::{
    self, CancelRunResponse, PollRun, RunControlRequest, SandboxRun, SandboxRunResponse,
    StartRun,
};
use tinybus::Proxy;

struct Runs {
    connection: Connection,
}

#[tinybus::interface(name = "ai.tinyhumans.tinyflows.Runs")]
impl Runs {
    /// Validate and reserve a run before acknowledging it to the host.
    async fn start_run(&self, request: StartRun) -> Result<runs::StartRunResponse> {
        super::run_manager::start(&self.connection, request)
            .await
            .map_err(|error| Error::failed(error.to_string()))
    }

    /// Read persisted run events after a monotonically increasing cursor.
    async fn poll_run(&self, request: RunControlRequest) -> Result<PollRun> {
        super::run_manager::poll(&self.connection, request)
            .await
            .map_err(|error| Error::failed(error.to_string()))
    }

    /// Signal a live run or atomically cancel a parked run.
    async fn cancel_run(&self, request: RunControlRequest) -> Result<CancelRunResponse> {
        super::run_manager::cancel(&self.connection, request)
            .await
            .map_err(|error| Error::failed(error.to_string()))
    }

    /// Execute an unsaved graph with deterministic mocks and a preflight-only
    /// host ToolInvoke callback. This path never creates live run state.
    async fn sandbox_run(&self, request: SandboxRun) -> Result<SandboxRunResponse> {
        sandbox_run(&self.connection, request).await
    }
}

struct SandboxToolInvoker {
    proxy: Proxy,
}

#[async_trait]
impl ToolInvoker for SandboxToolInvoker {
    async fn invoke(&self, slug: &str, args: Value, conn: Option<&str>) -> EngineResult<Value> {
        self.proxy
            .call(runs::caps::TOOL_INVOKE, &(slug, args, conn))
            .await
            .map_err(|error| EngineError::Capability(error.to_string()))
    }
}

async fn sandbox_run(connection: &Connection, request: SandboxRun) -> Result<SandboxRunResponse> {
    let graph: WorkflowGraph = tinyflows::authoring::validate_and_migrate_graph(request.graph)
        .map_err(|error| Error::failed(format!("Cannot dry-run an invalid graph: {error}")))?;
    let compiled = tinyflows::compiler::compile(&graph)
        .map_err(|error| Error::failed(format!("Draft graph failed to compile: {error}")))?;
    let proxy = connection
        .proxy(
            &request.tool_invoker.destination,
            &request.tool_invoker.object_path,
            runs::CAPS_INTERFACE,
        )
        .map_err(|error| Error::failed(error.to_string()))?
        .with_timeout(Duration::from_secs(30));
    let tool_invoker = Arc::new(SandboxToolInvoker { proxy }) as Arc<dyn ToolInvoker>;
    let capabilities = caps::mock_schema_aware::sandbox_capabilities(tool_invoker);
    let execution = tinyflows::sandbox::execute(
        &graph,
        &compiled,
        request.input,
        &capabilities,
        Duration::from_secs(30),
    );
    let outcome = match execution.await {
        Ok(outcome) => outcome,
        Err(tinyflows::sandbox::SandboxError::TimedOut) => {
            return Err(Error::failed("Sandbox dry-run timed out after 30s"));
        }
        Err(tinyflows::sandbox::SandboxError::Failed {
            message,
            unverifiable_bindings,
        }) => {
            let result = if !unverifiable_bindings.is_empty() {
                json!({
                    "sandbox": true,
                    "ok": false,
                    "error": message,
                    "unverifiable_bindings": unverifiable_bindings,
                    "note": "SANDBOX (mock) output — a tool_call node aborted because a required arg binds to the output of an upstream Composio tool_call, which the sandbox can only ECHO (it never produces real tool output fields). See unverifiable_bindings: each MAY already be wired correctly — confirm the path with get_tool_contract {{ slug }} (output_fields / primary_array_path; Composio results nest under .item.json.data.) or get_tool_output_sample {{ slug, args }} instead of re-wiring blindly. No real side effects occurred."
                })
            } else {
                json!({
                    "sandbox": true,
                    "ok": false,
                    "error": message,
                    "note": "SANDBOX (mock) output — a node errored during simulation. No real side effects occurred."
                })
            };
            return Ok(SandboxRunResponse { result });
        }
    };
    let report = outcome.report;
    let result = if report.is_failing() {
        json!({
            "sandbox": true,
            "ok": false,
            "null_resolutions": report.null_resolutions,
            "agent_prompt_nulls": report.agent_prompt_nulls,
            "agent_input_context_nulls": report.agent_input_context_nulls,
            "node_errors": report.node_errors,
            "routing_divergence_warnings": report.routing_divergence_warnings,
            "permissions_manifest": [],
            "message": "These tool_call args resolved to null, an agent node's prompt or input_context resolved to null (an EMPTY prompt or no upstream data at all), or a tool_call node failed during the sandbox run. Fix or rewire the named fields and nodes. Routing warnings are advisory and do not fail the dry run."
        })
    } else {
        json!({
            "sandbox": true,
            "ok": true,
            "output": outcome.output,
            "pending_approvals": outcome.pending_approvals,
            "null_resolutions": report.null_resolutions,
            "agent_prompt_nulls": report.agent_prompt_nulls,
            "agent_input_context_nulls": report.agent_input_context_nulls,
            "node_errors": report.node_errors,
            "routing_divergence_warnings": report.routing_divergence_warnings,
            "permissions_manifest": [],
            "note": "SANDBOX (mock) output — LLM/tool/HTTP/code nodes returned deterministic echoes; NO real side effects occurred. This checks wiring/routing only, not whether real integrations work."
        })
    };
    Ok(SandboxRunResponse { result })
}

pub(super) async fn setup(connection: &Connection) -> Result<()> {
    connection
        .serve_at(
            runs::OBJECT_PATH.try_into()?,
            Runs {
                connection: connection.clone(),
            },
        )
        .await
}
