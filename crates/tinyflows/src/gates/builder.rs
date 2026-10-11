//! Ordered authoring gates with host-owned catalogs and policy as ports.

use crate::{caps::WorkflowResolver, error::Result, model::WorkflowGraph};

/// Host-specific gate stages, in increasing cost order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HostGateStage {
    /// Native tool argument policy, such as confined upload paths.
    ToolPolicy,
    /// References to agents in the host's registry.
    AgentReferences,
    /// Tool references to the host's connected accounts.
    Connections,
    /// Action availability and required arguments in the host's tool catalog.
    ToolContracts,
}

/// Product-specific checks and saved-workflow lookup supplied by the host.
#[async_trait::async_trait]
pub trait BuilderGateHost: WorkflowResolver {
    /// Check one stage without running later, potentially more expensive stages.
    ///
    /// Empty diagnostics mean success. Diagnostics are returned verbatim;
    /// transport or store failures propagate as errors instead of passing a gate.
    async fn check(&self, stage: HostGateStage, graph: &WorkflowGraph) -> Result<Vec<String>>;
}

/// Run the author-time gate stack, returning every diagnostic from its first
/// failing stage. Structural validation must run before this function.
///
/// Checks engine topology (inline and saved children), portable authoring
/// gates, tool policy, agent references, connections, tool contracts, then mock
/// argument preflight. Native prefixes exempt host-only actions from the last
/// stage. Provider connectivity is intentionally left to runtime/advisory checks.
///
/// # Errors
/// Propagates host gate failures. Saved-workflow lookup failures retain the
/// existing compatibility behavior: skip unavailable graphs and leave their
/// missing-reference diagnosis to runtime.
pub async fn builder_failures(
    graph: &WorkflowGraph,
    host: &dyn BuilderGateHost,
    native_slug_prefixes: &[&str],
) -> Result<Vec<String>> {
    let compatibility: Vec<String> = crate::compat::errors(graph)
        .into_iter()
        .map(|error| format!("{}: {}", error.code, error.message))
        .collect();
    if !compatibility.is_empty() {
        return Ok(compatibility);
    }
    let saved = crate::compat::referenced_workflow_errors_async(graph, host).await;
    if !saved.is_empty() {
        return Ok(saved);
    }
    let portable = super::authoring_failures(graph);
    if !portable.is_empty() {
        return Ok(portable);
    }
    for stage in [
        HostGateStage::ToolPolicy,
        HostGateStage::AgentReferences,
        HostGateStage::Connections,
        HostGateStage::ToolContracts,
    ] {
        let failures = host.check(stage, graph).await?;
        if !failures.is_empty() {
            return Ok(failures);
        }
    }
    Ok(crate::preflight::unresolvable_tool_args_with_resolver(
        graph,
        native_slug_prefixes,
        Some(host),
    )
    .await)
}
