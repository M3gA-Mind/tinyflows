//! Wire contracts for workflow run control and host-provided capabilities.
//!
//! These records are serde-only. A `CapsHost` address is supplied by the
//! trusted host for one run; the module uses that exact address for every
//! callback and never chooses an ambient default from graph or trigger data.

use serde::{Deserialize, Serialize};
use serde_json::Value;

mod caps_types;
mod caps_agent_types;

pub use caps_agent_types::*;
pub use caps_types::*;

/// Interface for run control on the shared module bus name.
pub const INTERFACE: &str = "ai.tinyhumans.tinyflows.Runs";
/// Run-control object path.
pub const OBJECT_PATH: &str = "/ai/tinyhumans/tinyflows/Runs";
/// Version of the run-control contract.
pub const CONTRACT_VERSION: u32 = 1;
/// Run-control members, served by the later run-management slice.
pub const METHODS: &[&str] = &["StartRun", "ResumeRun", "CancelRun", "PollRun"];

/// Stable member names.
pub mod methods {
    /// Start a run of a graph; returns a run id immediately.
    pub const START_RUN: &str = "StartRun";
    /// Resume a parked run with approvals or inputs.
    pub const RESUME_RUN: &str = "ResumeRun";
    /// Cooperatively cancel a run.
    pub const CANCEL_RUN: &str = "CancelRun";
    /// Read run events after a cursor.
    pub const POLL_RUN: &str = "PollRun";
}

/// Interface the host serves so the module can reach every host capability.
pub const CAPS_INTERFACE: &str = "ai.tinyhumans.tinyflows.CapsHost";
/// Version of the host-served capability callback contract.
pub const CAPS_CONTRACT_VERSION: u32 = 1;
/// Maximum progress records accepted in one `AgentEvents` page.
pub const MAX_AGENT_EVENTS_PAGE: usize = 256;
/// Maximum progress records folded into one agent transcript.
pub const MAX_AGENT_TRANSCRIPT_EVENTS: usize = 10_000;

/// Host callback members, in stable interface order.
pub const CAPS_METHODS: &[&str] = &[
    caps::LLM_COMPLETE,
    caps::TOOL_INVOKE,
    caps::HTTP_REQUEST,
    caps::CODE_RUN,
    caps::SHELL_RUN,
    caps::MEMORY_RECALL,
    caps::MEMORY_FLAVOUR,
    caps::MEMORY_PEOPLE,
    caps::MEMORY_REMEMBER,
    caps::MEMORY_FORGET,
    caps::AGENT_RUN,
    caps::AGENT_EVENTS,
    caps::RESOLVE_AGENT,
    caps::LIST_AGENTS,
    caps::RESOLVE_CONTEXT,
    caps::RESOLVE_TOOLS,
    caps::RESOLVE_WORKDIR,
    caps::APPROVAL_DECIDE,
    caps::APPROVAL_CANCEL,
];

/// `StartRun` request; the host callback address is intentionally not a field
/// in this graph-shaped request and is added by the trusted run manager.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct StartRun {
    /// Migrated graph JSON.
    pub graph: Value,
    /// Trigger payload.
    pub trigger: Value,
    /// Optional host-supplied id for an idempotent restart.
    pub run_id: Option<String>,
}

/// `ResumeRun` request.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct ResumeRun {
    /// Parked run.
    pub run_id: String,
    /// Approved node ids.
    pub approvals: Vec<String>,
}

/// One run event returned by `PollRun`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct RunEvent {
    /// Monotonic position; pass the last seen value as the next cursor.
    pub seq: u64,
    /// Event kind (`step`, `parked`, `finished`, `agent_progress`).
    pub kind: String,
    /// Event payload.
    pub data: Value,
}

/// `PollRun` response.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct PollRun {
    /// Events after the requested cursor.
    pub events: Vec<RunEvent>,
    /// Whether the run reached a terminal status.
    pub done: bool,
}

/// Exact address of a callback object created by the trusted host.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct CallbackAddress {
    /// Bus destination of the callback object.
    pub destination: String,
    /// Callback object path.
    pub object_path: String,
}

/// Address to use for one run's capability calls. The host binds this address
/// to the flow, run, approval policy and owner scope before passing it to the
/// module. None of those values are accepted from the graph or trigger.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct CapsHost {
    /// Exact object address chosen by the trusted host for this run.
    pub callback: CallbackAddress,
}

/// Stable `CapsHost` member names and record conventions.
pub mod caps {
    /// `LlmProvider::complete(request, conn) -> Value`.
    pub const LLM_COMPLETE: &str = "LlmComplete";
    /// `ToolInvoker::invoke(slug, args, conn) -> Value`.
    pub const TOOL_INVOKE: &str = "ToolInvoke";
    /// `HttpClient::request(request, conn) -> Value`.
    pub const HTTP_REQUEST: &str = "HttpRequest";
    /// `CodeRunner::run(language, source, input) -> Value`.
    pub const CODE_RUN: &str = "CodeRun";
    /// `ShellRunner::run(request) -> ShellOutcome`.
    pub const SHELL_RUN: &str = "ShellRun";
    /// Memory recall method.
    pub const MEMORY_RECALL: &str = "MemoryRecall";
    /// Memory flavour method.
    pub const MEMORY_FLAVOUR: &str = "MemoryFlavour";
    /// Memory people method.
    pub const MEMORY_PEOPLE: &str = "MemoryPeople";
    /// Memory remember method.
    pub const MEMORY_REMEMBER: &str = "MemoryRemember";
    /// Memory forget method.
    pub const MEMORY_FORGET: &str = "MemoryForget";
    /// Start a host-owned agent turn; returns an opaque event handle.
    pub const AGENT_RUN: &str = "AgentRun";
    /// Read agent events after a cursor.
    pub const AGENT_EVENTS: &str = "AgentEvents";
    /// Resolve a host agent definition; `None` means ordinary absence.
    pub const RESOLVE_AGENT: &str = "ResolveAgent";
    /// List host agent definitions.
    pub const LIST_AGENTS: &str = "ListAgents";
    /// Resolve host context; `None` means ordinary absence.
    pub const RESOLVE_CONTEXT: &str = "ResolveContext";
    /// Expand allowed tool grants into descriptors.
    pub const RESOLVE_TOOLS: &str = "ResolveTools";
    /// Resolve an agent work directory.
    pub const RESOLVE_WORKDIR: &str = "ResolveWorkdir";
    /// Idempotently create or fetch an approval request.
    pub const APPROVAL_DECIDE: &str = "ApprovalDecide";
    /// Best-effort withdrawal of an approval request.
    pub const APPROVAL_CANCEL: &str = "ApprovalCancel";
}

#[cfg(test)]
#[path = "runs_tests.rs"]
mod tests;
