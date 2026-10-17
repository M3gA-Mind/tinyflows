//! Execute spawned workflow tasks with the run's host-served capabilities.

use std::sync::{Arc, OnceLock, Weak};

use async_trait::async_trait;
use serde_json::Value;
use tinyflows::{
    caps::{Capabilities, TaskExecutor, TaskRunner, TaskSpec},
    error::{EngineError, Result},
};

use super::capability_error;

pub(super) struct CapabilityTaskExecutor {
    capabilities: Capabilities,
    task_runner: Arc<TaskRunnerLink>,
}

#[derive(Default)]
pub(super) struct TaskRunnerLink {
    runner: OnceLock<Weak<dyn TaskRunner>>,
}

impl TaskRunnerLink {
    pub(super) fn bind(&self, runner: &Arc<dyn TaskRunner>) {
        let _ = self.runner.set(Arc::downgrade(runner));
    }

    pub(super) fn upgrade(&self) -> Option<Arc<dyn TaskRunner>> {
        self.runner.get().and_then(Weak::upgrade)
    }
}

impl CapabilityTaskExecutor {
    pub(super) fn new(capabilities: Capabilities, task_runner: Arc<TaskRunnerLink>) -> Self {
        Self {
            capabilities,
            task_runner,
        }
    }
}

#[async_trait]
impl TaskExecutor for CapabilityTaskExecutor {
    async fn execute(&self, spec: TaskSpec) -> Result<Value> {
        match spec {
            TaskSpec::Tool { slug, args } => {
                self.capabilities.tools.invoke(&slug, args, None).await
            }
            TaskSpec::Http { request } => self.capabilities.http.request(request, None).await,
            TaskSpec::Workflow { graph, input } => {
                let graph: tinyflows::model::WorkflowGraph = serde_json::from_value(graph)
                    .map_err(|error| {
                        EngineError::Capability(format!("invalid child workflow: {error}"))
                    })?;
                tinyflows::compat::ensure_compatible(&graph).map_err(capability_error)?;
                let compiled = tinyflows::compiler::compile(&graph).map_err(capability_error)?;
                let mut capabilities = self.capabilities.clone();
                capabilities.tasks = Some(self.task_runner.upgrade().ok_or_else(|| {
                    capability_error("task runner was dropped before child workflow execution")
                })?);
                let outcome = tinyflows::engine::run(&compiled, input, &capabilities)
                    .await
                    .map_err(capability_error)?;
                if outcome.cancelled {
                    return Err(capability_error("spawned child workflow was cancelled"));
                }
                if !outcome.pending_approvals.is_empty() {
                    return Err(capability_error(format!(
                        "spawned child workflow is waiting for approvals {:?}; task tickets cannot be resumed",
                        outcome.pending_approvals
                    )));
                }
                Ok(outcome.output)
            }
        }
    }
}
