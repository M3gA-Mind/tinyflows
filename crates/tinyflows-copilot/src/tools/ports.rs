//! The host seams the copilot's builder tools run over.
//!
//! The tools in this module hold every piece of authoring behaviour that is a
//! property of tinyflows (argument parsing, precedence between a draft, a saved
//! flow and an inline graph, the propose-only persistence contract, the wording
//! a model reads back). What they cannot know is *where* drafts and flows live,
//! which hard gates a host enforces on a graph, or which agents and node-kind
//! caveats exist in that host. Those answers come through the traits below, so
//! the same tool belt runs over a desktop workspace, a remote orchestrator or an
//! in-memory test double.
//!
//! Every method reports failure as a `String`: the tools surface it to the model
//! verbatim as the tail of an error sentence, so a host formats its own error
//! type once, at the seam.

use async_trait::async_trait;
use serde_json::Value;
use tinyflows::caps::ToolInvoker;
use tinyflows::catalog::NodeKindContract;
use tinyflows::model::WorkflowGraph;
use tinyflows_catalog::types::FlowSuggestion;
use tinyflows_catalog::{DraftOrigin, Flow, FlowDraft, FlowRevision, FlowRun, FlowValidation};

/// A port call that failed, as the sentence the model will read.
pub type PortResult<T> = Result<T, String>;

/// A written value plus the explanatory log lines the host produced alongside it
/// (for example the manual-to-automatic auto-disarm message of a save).
#[derive(Debug, Clone)]
pub struct Written<T> {
    /// The persisted value.
    pub value: T,
    /// Host log lines worth relaying to the model, oldest first.
    pub logs: Vec<String>,
}

/// Working drafts: the durable copy a builder iterates on across turns.
#[async_trait]
pub trait DraftStore: Send + Sync {
    /// Loads draft `id`.
    async fn get(&self, id: &str) -> PortResult<FlowDraft>;
    /// Creates a draft, optionally linked to the saved flow it derives from.
    async fn create(
        &self,
        flow_id: Option<String>,
        name: String,
        graph: Value,
        origin: DraftOrigin,
    ) -> PortResult<FlowDraft>;
    /// Overwrites a draft's name and/or graph; `None` leaves a field alone.
    async fn update(
        &self,
        id: &str,
        name: Option<String>,
        graph: Option<Value>,
    ) -> PortResult<FlowDraft>;
}

/// Saved flows and their runs.
#[async_trait]
pub trait FlowStore: Send + Sync {
    /// Loads saved flow `id`.
    async fn get(&self, id: &str) -> PortResult<Flow>;
    /// Lists every saved flow.
    async fn list(&self) -> PortResult<Vec<Flow>>;
    /// A saved flow's graph, or `None` when no such flow exists.
    async fn load_graph(&self, id: &str) -> PortResult<Option<WorkflowGraph>>;
    /// Persists a NEW flow from `graph`.
    async fn create(&self, name: String, graph: Value, require_approval: bool) -> PortResult<Flow>;
    /// Writes `graph` (and optionally a new `name`) onto an existing flow.
    async fn update(
        &self,
        id: &str,
        name: Option<String>,
        graph: Option<Value>,
    ) -> PortResult<Written<Flow>>;
    /// Enables or disables a flow.
    async fn set_enabled(&self, id: &str, enabled: bool) -> PortResult<Flow>;
    /// Clones a flow into an independent, disabled copy.
    async fn duplicate(&self, id: &str) -> PortResult<Flow>;
    /// A flow's prior graph snapshots, newest first.
    async fn history(&self, id: &str, limit: usize) -> PortResult<Vec<FlowRevision>>;
    /// A flow's recent runs, newest first.
    async fn list_runs(&self, flow_id: &str, limit: usize) -> PortResult<Vec<FlowRun>>;
    /// One run's record.
    async fn get_run(&self, run_id: &str) -> PortResult<FlowRun>;
    /// Advances a run parked on approval.
    async fn resume(
        &self,
        flow_id: &str,
        run_id: &str,
        approve: Vec<String>,
        reject: Vec<String>,
    ) -> PortResult<Value>;
    /// Stops an in-flight or parked run.
    async fn cancel_run(&self, run_id: &str) -> PortResult<Value>;
}

/// The validation, gate and sandbox policy a host enforces on a graph.
#[async_trait]
pub trait BuilderGates: Send + Sync {
    /// Structural validation of a candidate graph, every problem at once.
    async fn validate(&self, graph: Value) -> FlowValidation;
    /// The full hard-gate stack; empty means every gate passed.
    async fn run_gates(&self, graph: &WorkflowGraph) -> Vec<String>;
    /// Structural validation plus [`run_gates`](Self::run_gates), as one verdict
    /// for a create or save.
    async fn strict_gate(&self, graph: &Value) -> PortResult<()>;
    /// Engine-compatibility errors the host's configuration adds to the generic
    /// ones; empty when the graph is acceptable.
    async fn engine_compatibility_errors(&self, graph: &WorkflowGraph) -> Vec<String>;
    /// Non-fatal wiring warnings (for example unwired required tool arguments).
    async fn wiring_warnings(&self, graph: &WorkflowGraph) -> Vec<String>;
    /// The outbound-effect approvals a run of `graph` would need.
    async fn approval_manifest(&self, graph: &WorkflowGraph) -> Vec<Value>;
    /// The `workflow_proposal` payload for a graph that passed validation, or
    /// the gate-failure message to hand back to the model.
    #[allow(clippy::too_many_arguments)]
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
    ) -> Result<Value, String>;
    /// Host-bound tool preflight used by sandbox runs. It must validate only;
    /// it must never invoke a real integration or cause an external effect.
    fn sandbox_tool_invoker(&self) -> std::sync::Arc<dyn ToolInvoker>;
}

/// One selectable agent definition, already rendered the way the tool returns it.
#[derive(Debug, Clone)]
pub struct AgentDefinition {
    /// Registry id, the value of an `agent` node's `agent_ref`.
    pub id: String,
    /// Display name.
    pub name: String,
    /// What the agent is for.
    pub description: String,
    /// Model identifier, or `null`.
    pub model: Value,
    /// Tool allowlist.
    pub tools: Value,
    /// Free-form tags.
    pub tags: Value,
}

/// The agents an `agent` node can select through `agent_ref`.
#[async_trait]
pub trait AgentDirectory: Send + Sync {
    /// Every enabled, selectable agent definition.
    async fn list(&self) -> PortResult<Vec<AgentDefinition>>;
}

/// The node-kind catalog as this host presents it.
///
/// The portable contracts are `tinyflows::catalog`'s; a host appends the facts
/// only it knows (what a `tool_call` slug resolves to, which triggers dispatch).
pub trait NodeKindSource: Send + Sync {
    /// Every contract, in catalog order.
    fn all(&self) -> Vec<NodeKindContract>;
    /// One kind's contract, or `None` when `kind` is not a node kind.
    fn get(&self, kind: &str) -> Option<NodeKindContract>;
}

/// The portable catalog with no host overlay.
#[derive(Debug, Clone, Copy, Default)]
pub struct PortableNodeKinds;

impl NodeKindSource for PortableNodeKinds {
    fn all(&self) -> Vec<NodeKindContract> {
        tinyflows::catalog::all_contracts()
    }

    fn get(&self, kind: &str) -> Option<NodeKindContract> {
        tinyflows::catalog::contract_for(kind)
    }
}

/// Where discovered workflow suggestions are recorded.
#[async_trait]
pub trait SuggestionStore: Send + Sync {
    /// Inserts or refreshes `suggestions`, returning how many were written.
    async fn upsert(&self, suggestions: &[FlowSuggestion]) -> PortResult<usize>;
}
