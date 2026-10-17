//! Serde-only request and response types for non-agent capability callbacks.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

/// Language accepted by a code-run callback.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum CodeLanguage {
    /// Existing JavaScript code-node convention.
    #[serde(rename = "javascript")]
    JavaScript,
    /// Existing Python code-node convention.
    #[serde(rename = "python")]
    Python,
}

/// `CodeRun(language, source, input)`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct CodeRunRequest {
    /// Selected source language.
    pub language: CodeLanguage,
    /// Source text, forwarded without wrapping or rewriting.
    pub source: String,
    /// JSON input passed to the code runner.
    pub input: Value,
}

/// Script source accepted by the closed shell interpreter contract.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum ShellScript {
    /// Inline script body.
    Inline(String),
    /// Untrusted path to a script file.
    Path(String),
}

/// Shell interpreter accepted by the engine.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ShellInterpreter {
    /// POSIX shell.
    #[default]
    Sh,
    /// GNU Bash.
    Bash,
}

/// Typed shell request. Paths and environment are untrusted workflow input.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct ShellRunRequest {
    /// Closed interpreter choice.
    pub interpreter: ShellInterpreter,
    /// Inline script or untrusted script path.
    pub script: ShellScript,
    /// Untrusted working directory, if declared.
    pub cwd: Option<String>,
    /// Declared environment entries.
    pub env: BTreeMap<String, String>,
    /// Node input items as JSON.
    pub input: Value,
}

/// Shell process outcome; a nonzero exit is a successful callback response.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ShellRunOutcome {
    /// Exit code, negative when signalled.
    pub exit_code: i32,
    /// Captured standard output.
    pub stdout: String,
    /// Captured standard error.
    pub stderr: String,
}

/// `MemoryRecall(scope, query, opts)`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct MemoryRecallRequest {
    /// Host-defined scope.
    pub scope: String,
    /// Search text.
    pub query: String,
    /// Operation and optional memory-node settings.
    pub opts: Value,
}

/// `MemoryFlavour(slug)`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct MemoryFlavourRequest {
    /// Host-defined flavour key.
    pub slug: String,
}

/// `MemoryPeople(query)`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct MemoryPeopleRequest {
    /// Optional search term; absent means default listing.
    pub query: Option<String>,
}

/// `MemoryRemember(scope, key, value)`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct MemoryRememberRequest {
    /// Host-defined memory scope.
    pub scope: String,
    /// Memory key.
    pub key: String,
    /// JSON value to persist.
    pub value: Value,
}

/// `MemoryForget(scope, key)`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct MemoryForgetRequest {
    /// Host-defined memory scope.
    pub scope: String,
    /// Memory key.
    pub key: String,
}

/// One approval's presentation data.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct ApprovalSubject {
    /// Host-defined rendering hint.
    pub kind: String,
    /// Payload to review.
    pub value: Value,
}

/// `ApprovalDecide(request)`; request ids are idempotency keys.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct ApprovalRequest {
    /// Stable create-or-fetch key.
    pub request_id: String,
    /// Graph node requesting review.
    pub node_id: String,
    /// Workflow run id when available.
    pub run_id: Option<String>,
    /// Reviewer-facing title.
    pub title: Option<String>,
    /// Reviewer-facing prompt.
    pub prompt: Option<String>,
    /// Content being reviewed.
    pub subject: ApprovalSubject,
    /// Host-resolved reviewer references.
    pub assignees: Vec<String>,
    /// Opaque metadata.
    pub metadata: Value,
}

/// Human decision data.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct ApprovalDecision {
    /// True for approve, false for reject.
    pub approved: bool,
    /// Host-defined reviewer reference.
    pub decided_by: Option<String>,
    /// Optional reviewer note.
    pub comment: Option<String>,
    /// Edited payload, or absent when unchanged.
    pub payload: Option<Value>,
}

/// Create-or-fetch status for an approval request.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(tag = "status", content = "decision", rename_all = "snake_case")]
pub enum ApprovalOutcome {
    /// Request is open; the module parks the run and retries after resume.
    Pending,
    /// Request has a final human decision.
    Decided(ApprovalDecision),
}

/// `ApprovalCancel(request_id, reason)`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ApprovalCancelRequest {
    /// Idempotency key of the request being withdrawn.
    pub request_id: String,
    /// Why the workflow no longer needs a response.
    pub reason: String,
}
