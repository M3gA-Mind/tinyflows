//! TinyBus entry points for module-owned workflow run lifecycle.

use std::sync::Arc;
use std::time::Duration;
use tinybus::{Connection, Error, Result};

use async_trait::async_trait;
use serde_json::Value;
use tinybus::Proxy;
use tinyflows::caps::{self, ToolInvoker};
use tinyflows::error::{EngineError, Result as EngineResult};
use tinyflows::model::WorkflowGraph;
use tinyflows_bus::runs::{
    self, BootSweep, BootSweepResponse, CancelRunResponse, ExpireParked, ExpireParkedResponse,
    PollRun, ResumeRun, ResumeRunResponse, RunControlRequest, SandboxRun, SandboxRunResponse,
    StartRun,
};

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

    /// Claim a parked run and continue its pinned checkpoint with host decisions.
    async fn resume_run(&self, request: ResumeRun) -> Result<ResumeRunResponse> {
        super::run_manager::resume(&self.connection, request)
            .await
            .map_err(|error| Error::failed(error.to_string()))
    }

    /// Cancel parked runs whose approval window has expired.
    async fn expire_parked(&self, request: ExpireParked) -> Result<ExpireParkedResponse> {
        super::run_manager::expire_parked(&self.connection, request)
            .await
            .map_err(|error| Error::failed(error.to_string()))
    }

    /// Reconcile orphaned local runs after module startup.
    async fn boot_sweep(&self, request: BootSweep) -> Result<BootSweepResponse> {
        super::run_manager::boot_sweep(&self.connection, request)
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
    mock: Arc<dyn ToolInvoker>,
}

#[async_trait]
impl ToolInvoker for SandboxToolInvoker {
    async fn invoke(&self, slug: &str, args: Value, conn: Option<&str>) -> EngineResult<Value> {
        // The callback's successful payload is only a preflight acknowledgement;
        // simulated tool output always comes from this module's MockTools.
        let _: Value = self
            .proxy
            .call(runs::caps::TOOL_INVOKE, &(slug, args.clone(), conn))
            .await
            .map_err(|error| EngineError::Capability(error.to_string()))?;
        self.mock.invoke(slug, args, conn).await
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
    let mock = caps::mock::mock_capabilities().tools;
    let tool_invoker = Arc::new(SandboxToolInvoker { proxy, mock }) as Arc<dyn ToolInvoker>;
    let capabilities = caps::mock_schema_aware::sandbox_capabilities(tool_invoker);
    let execution = tinyflows::sandbox::execute(
        &graph,
        &compiled,
        request.input,
        &capabilities,
        Duration::from_secs(30),
    );
    let result = tinyflows::sandbox::report(execution.await, 30, request.permissions_manifest)
        .map_err(Error::failed)?;
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
