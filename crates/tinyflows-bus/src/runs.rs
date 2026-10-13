//! Skeleton vocabulary for workflow run control (run / resume / cancel).
//!
//! Not yet served by `tinyflows-module`; see `docs/tinyflows-bus-design.md`.
//! Synchronous, I/O-free, JSON-shaped. Streaming is expressed as host
//! callbacks and a poll cursor, never as a transport stream.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Interface for run control on the shared module bus name.
pub const INTERFACE: &str = "ai.tinyhumans.tinyflows.Runs";
/// Served run-control object path.
pub const OBJECT_PATH: &str = "/ai/tinyhumans/tinyflows/Runs";
/// Version of the run-control vocabulary.
pub const CONTRACT_VERSION: u32 = 1;
/// Run-control members.
pub const METHODS: &[&str] = &["StartRun", "ResumeRun", "CancelRun", "PollRun"];

/// Stable member names.
pub mod methods {
    /// Start a run of a graph; returns a run id immediately.
    pub const START_RUN: &str = "StartRun";
    /// Resume a parked run with approvals or inputs.
    pub const RESUME_RUN: &str = "ResumeRun";
    /// Cooperatively cancel a run.
    pub const CANCEL_RUN: &str = "CancelRun";
    /// Read run events after a cursor (the streaming surface).
    pub const POLL_RUN: &str = "PollRun";
}

/// Interface the host serves so the module can reach every capability trait.
pub const CAPS_INTERFACE: &str = "ai.tinyhumans.tinyflows.CapsHost";

/// Host-callback members, one per `tinyflows::caps` trait method.
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
    /// `MemoryProvider::{recall,flavour,people,remember,forget}`.
    pub const MEMORY: &[&str] = &["MemoryRecall", "MemoryFlavour", "MemoryPeople", "MemoryRemember", "MemoryForget"];
    /// `WorkflowResolver::resolve(id) -> graph`.
    pub const RESOLVE_WORKFLOW: &str = "ResolveWorkflow";
    /// `StateStore::{load,store}`.
    pub const STATE: &[&str] = &["StateLoad", "StateStore"];
    /// `ApprovalProvider::{decide,cancel}`.
    pub const APPROVAL: &[&str] = &["ApprovalDecide", "ApprovalCancel"];
    /// `TaskRunner::{start,poll,cancel}`.
    pub const TASK: &[&str] = &["TaskStart", "TaskPoll", "TaskCancel"];
    /// `AgentRunner::run`; progress returns through [`AGENT_EVENTS`].
    pub const AGENT_RUN: &str = "AgentRun";
    /// Cursor read of agent progress events for an in-flight `AgentRun`.
    pub const AGENT_EVENTS: &str = "AgentEvents";
}

/// `StartRun` request.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct StartRun {
    /// Migrated graph JSON.
    pub graph: Value,
    /// Trigger payload.
    pub trigger: Value,
    /// Optional run id supplied by the host (idempotent restart).
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
