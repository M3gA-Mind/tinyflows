//! Deterministic execution used by builder dry-runs and the module sandbox.
//!
//! The caller supplies the only capability that may leave the module (tool
//! invocation); every report and failure classification is shared here.

use std::time::Duration;

use serde_json::Value;

use crate::authoring_report::{self, AuthoringReport};
use crate::caps::Capabilities;
use crate::model::WorkflowGraph;
use crate::observability::CapturingObserver;

/// The settled result of a mock-only graph execution.
#[derive(Debug)]
pub struct SandboxOutcome {
    /// Final merged state from the graph.
    pub output: Value,
    /// Approval requests left by the mock approval surface.
    pub pending_approvals: Vec<String>,
    /// Diagnostics shared by every sandbox entry point.
    pub report: AuthoringReport,
}

/// Why a sandbox execution did not settle.
#[derive(Debug)]
pub enum SandboxError {
    /// Execution exceeded its caller-selected deadline.
    TimedOut,
    /// Engine execution failed, with bindings the echo tool cannot verify.
    Failed {
        /// Engine error text.
        message: String,
        /// Required args bound to an upstream tool's real output.
        unverifiable_bindings: Vec<authoring_report::NullResolutionEntry>,
    },
}

/// Executes a compiled graph and builds the canonical authoring report.
///
/// Compile and validate before calling. The `Capabilities` passed here should
/// contain deterministic mocks for every slot except an optional host-bound
/// tool preflight callback. No engine state is persisted by this function.
pub async fn execute(
    graph: &WorkflowGraph,
    compiled: &crate::compiler::CompiledWorkflow,
    input: Value,
    capabilities: &Capabilities,
    timeout: Duration,
) -> Result<SandboxOutcome, SandboxError> {
    let observer = std::sync::Arc::new(CapturingObserver::default());
    let observer_dyn: std::sync::Arc<dyn crate::observability::RunObserver> = observer.clone();
    let run = crate::engine::run_with_observer(compiled, input, capabilities, &observer_dyn);
    let outcome = match tokio::time::timeout(timeout, run).await {
        Ok(Ok(outcome)) => outcome,
        Ok(Err(error)) => {
            return Err(SandboxError::Failed {
                message: error.to_string(),
                unverifiable_bindings: authoring_report::unverifiable_bindings(
                    graph,
                    &observer.steps(),
                ),
            });
        }
        Err(_) => return Err(SandboxError::TimedOut),
    };
    let report = AuthoringReport::build(graph, &observer.steps(), &outcome.output);
    Ok(SandboxOutcome {
        output: outcome.output,
        pending_approvals: outcome.pending_approvals,
        report,
    })
}

#[cfg(test)]
#[path = "sandbox_tests.rs"]
mod tests;
