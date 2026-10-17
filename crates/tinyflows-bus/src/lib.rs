//! Transport-free vocabulary for the loadable workflow authoring module.
//!
//! Graphs cross the boundary as JSON so hosts do not link the engine to read
//! authoring reports. Migration and interpretation belong to the module.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Module bus identity.
pub const BUS_NAME: &str = "ai.tinyhumans.tinyflows.Authoring";
/// Authoring interface name.
pub const INTERFACE: &str = "ai.tinyhumans.tinyflows.Authoring";
/// Served authoring object path.
pub const OBJECT_PATH: &str = "/ai/tinyhumans/tinyflows/Authoring";
/// Version of this wire contract.
pub const CONTRACT_VERSION: u32 = 3;
/// Authoring members provided by the module.
pub const METHODS: &[&str] = &[
    "ValidateGraph",
    "BindingFailures",
    "AuthoringFailures",
    "UnresolvableToolArgs",
    "BuilderFailures",
    "ImportGraph",
    "WiringWarnings",
];

/// Copilot service interface for the tool belt implemented by `tinyflows-copilot`.
pub mod copilot {
    use serde::{Deserialize, Serialize};
    use serde_json::Value;

    pub const INTERFACE: &str = "ai.tinyhumans.tinyflows.Copilot";
    pub const CONTRACT_VERSION: u32 = 1;
    pub const METHODS: &[&str] = &[
        "ToolCatalog",
        "InvokeTool",
        "RenderBuilderPrompt",
        "GetPrompt",
        "ListResources",
        "GetResource",
        "BuildTrailOffFallback",
        "CombineTrailOffFallback",
        "IsBackendOrInfrastructureFailure",
        "BackendRepairMessage",
        "TextLooksLikeQuestion",
    ];

    /// A per-invocation callback destination bound by the host to one trusted
    /// dispatch context. It is carried outside model-supplied tool arguments.
    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
    pub struct CopilotHost {
        pub bus_name: String,
        pub object_path: String,
    }

    /// Invoke one owning-library tool with its request-scoped host callbacks.
    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
    pub struct InvokeToolRequest {
        pub name: String,
        pub arguments: Value,
        pub host: CopilotHost,
        #[serde(default)]
        pub node_kinds: Vec<Value>,
    }

    pub const HOST_INTERFACE: &str = "ai.tinyhumans.tinyflows.CopilotHost";
    pub const HOST_METHODS: &[&str] = &["BuildProposal", "RunFlow", "CallPort"];

    pub mod host {
        pub const BUILD_PROPOSAL: &str = "BuildProposal";
        pub const RUN_FLOW: &str = "RunFlow";
        pub const CALL_PORT: &str = "CallPort";
    }

    pub mod port_operations {
        use super::PortOperation;
        pub const DRAFT_GET: PortOperation = PortOperation::DraftGet;
        pub const DRAFT_CREATE: PortOperation = PortOperation::DraftCreate;
        pub const DRAFT_UPDATE: PortOperation = PortOperation::DraftUpdate;
        pub const FLOWS_GET: PortOperation = PortOperation::FlowsGet;
        pub const FLOWS_LIST: PortOperation = PortOperation::FlowsList;
        pub const FLOWS_LOAD_GRAPH: PortOperation = PortOperation::FlowsLoadGraph;
        pub const FLOWS_CREATE: PortOperation = PortOperation::FlowsCreate;
        pub const FLOWS_UPDATE: PortOperation = PortOperation::FlowsUpdate;
        pub const FLOWS_SET_ENABLED: PortOperation = PortOperation::FlowsSetEnabled;
        pub const FLOWS_DUPLICATE: PortOperation = PortOperation::FlowsDuplicate;
        pub const FLOWS_HISTORY: PortOperation = PortOperation::FlowsHistory;
        pub const FLOWS_LIST_RUNS: PortOperation = PortOperation::FlowsListRuns;
        pub const FLOWS_GET_RUN: PortOperation = PortOperation::FlowsGetRun;
        pub const FLOWS_RESUME: PortOperation = PortOperation::FlowsResume;
        pub const FLOWS_CANCEL_RUN: PortOperation = PortOperation::FlowsCancelRun;
        pub const AGENTS_LIST: PortOperation = PortOperation::AgentsList;
        pub const SUGGESTIONS_UPSERT: PortOperation = PortOperation::SuggestionsUpsert;
        pub const GATES_VALIDATE: PortOperation = PortOperation::GatesValidate;
        pub const GATES_RUN: PortOperation = PortOperation::GatesRun;
        pub const GATES_STRICT: PortOperation = PortOperation::GatesStrict;
        pub const GATES_ENGINE_COMPATIBILITY: PortOperation =
            PortOperation::GatesEngineCompatibility;
        pub const GATES_WIRING_WARNINGS: PortOperation = PortOperation::GatesWiringWarnings;
        pub const GATES_APPROVAL_MANIFEST: PortOperation = PortOperation::GatesApprovalManifest;

        pub const ALL: &[PortOperation] = &[
            DRAFT_GET,
            DRAFT_CREATE,
            DRAFT_UPDATE,
            FLOWS_GET,
            FLOWS_LIST,
            FLOWS_LOAD_GRAPH,
            FLOWS_CREATE,
            FLOWS_UPDATE,
            FLOWS_SET_ENABLED,
            FLOWS_DUPLICATE,
            FLOWS_HISTORY,
            FLOWS_LIST_RUNS,
            FLOWS_GET_RUN,
            FLOWS_RESUME,
            FLOWS_CANCEL_RUN,
            AGENTS_LIST,
            SUGGESTIONS_UPSERT,
            GATES_VALIDATE,
            GATES_RUN,
            GATES_STRICT,
            GATES_ENGINE_COMPATIBILITY,
            GATES_WIRING_WARNINGS,
            GATES_APPROVAL_MANIFEST,
        ];
    }

    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
    pub struct PortCallRequest {
        pub operation: PortOperation,
        pub payload: Value,
    }

    #[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
    pub enum PortOperation {
        #[serde(rename = "draft.get")]
        DraftGet,
        #[serde(rename = "draft.create")]
        DraftCreate,
        #[serde(rename = "draft.update")]
        DraftUpdate,
        #[serde(rename = "flows.get")]
        FlowsGet,
        #[serde(rename = "flows.list")]
        FlowsList,
        #[serde(rename = "flows.load_graph")]
        FlowsLoadGraph,
        #[serde(rename = "flows.create")]
        FlowsCreate,
        #[serde(rename = "flows.update")]
        FlowsUpdate,
        #[serde(rename = "flows.set_enabled")]
        FlowsSetEnabled,
        #[serde(rename = "flows.duplicate")]
        FlowsDuplicate,
        #[serde(rename = "flows.history")]
        FlowsHistory,
        #[serde(rename = "flows.list_runs")]
        FlowsListRuns,
        #[serde(rename = "flows.get_run")]
        FlowsGetRun,
        #[serde(rename = "flows.resume")]
        FlowsResume,
        #[serde(rename = "flows.cancel_run")]
        FlowsCancelRun,
        #[serde(rename = "agents.list")]
        AgentsList,
        #[serde(rename = "suggestions.upsert")]
        SuggestionsUpsert,
        #[serde(rename = "gates.validate")]
        GatesValidate,
        #[serde(rename = "gates.run")]
        GatesRun,
        #[serde(rename = "gates.strict")]
        GatesStrict,
        #[serde(rename = "gates.engine_compatibility")]
        GatesEngineCompatibility,
        #[serde(rename = "gates.wiring_warnings")]
        GatesWiringWarnings,
        #[serde(rename = "gates.approval_manifest")]
        GatesApprovalManifest,
    }

    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
    pub struct BuildProposalRequest {
        pub retry_tool: String,
        pub name: String,
        pub graph: Value,
        pub require_approval: bool,
        pub revision: bool,
        pub instruction: Option<String>,
        pub draft_id: Option<String>,
        pub flow_id: Option<String>,
    }

    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
    pub struct RunFlowRequest {
        pub flow_id: String,
        pub input: Value,
        pub inputs: serde_json::Map<String, Value>,
    }

    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
    pub struct BuilderPromptRequest {
        pub mode: String,
        #[serde(default)]
        pub instruction: String,
        pub graph: Option<Value>,
        pub flow_id: Option<String>,
        pub run_id: Option<String>,
        pub error: Option<String>,
        #[serde(default)]
        pub failing_node_ids: Vec<String>,
    }

    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
    pub struct ToolResultSummary {
        pub tool_name: Option<String>,
        pub content: String,
    }

    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
    pub struct TrailOffRequest {
        pub history: Vec<ToolResultSummary>,
    }

    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
    pub struct TextResponse {
        pub text: String,
    }

    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
    pub struct ResourceFile {
        pub path: String,
        pub contents: String,
    }

    /// Flow validation result accepted from a scope-bound host callback.
    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Default)]
    pub struct FlowValidation {
        pub valid: bool,
        pub errors: Vec<String>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        pub error_details: Vec<FlowValidationError>,
        pub warnings: Vec<String>,
    }

    /// Structured validation diagnostic, matching the host catalog shape.
    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Default)]
    pub struct FlowValidationError {
        pub code: String,
        pub message: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub node_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub field: Option<String>,
    }

    /// Scope-resolved selectable agent contract used by `agents.list`.
    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
    pub struct AgentDefinition {
        pub id: String,
        pub name: String,
        pub description: String,
        pub model: Value,
        pub tools: Value,
        pub tags: Value,
    }

    /// Result of a flow update, including host-authored explanatory logs.
    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
    pub struct WrittenWorkflow {
        pub value: super::catalog::Workflow,
        pub logs: Vec<String>,
    }

    /// Stable tool metadata suitable for host registry construction.
    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
    pub struct ToolDescriptor {
        pub name: String,
        pub description: String,
        pub parameters_schema: Value,
        pub permission: String,
        pub external_effect: bool,
    }

    #[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
    pub struct ToolCatalogRequest {
        /// Host-rendered node-kind contracts used in propose tool descriptions.
        #[serde(default)]
        pub node_kinds: Vec<Value>,
    }

    #[cfg(test)]
    #[path = "copilot_tests.rs"]
    mod tests;
}

/// Context-bound host callback interface. It serves a saved-workflow lookup
/// and ordered product gates; the module never interprets host credentials.
pub const HOST_INTERFACE: &str = "ai.tinyhumans.tinyflows.AuthoringHost";

/// A host callback object scoped to this authoring request's identity/store.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct AuthoringHost {
    /// Destination that owns the context-bound callback object.
    pub bus_name: String,
    /// Object implementing [`HOST_INTERFACE`].
    pub object_path: String,
}

/// Host callback members and gate stage strings.
pub mod host {
    /// `ResolveWorkflow(id: String) -> Option<Value>`; unavailable graphs are skipped.
    pub const RESOLVE_WORKFLOW: &str = "ResolveWorkflow";
    /// `CheckGate(stage: String, graph: Value) -> Vec<String>`; errors fail closed.
    pub const CHECK_GATE: &str = "CheckGate";
    /// Native-tool argument policy.
    pub const TOOL_POLICY: &str = "tool_policy";
    /// Host agent registry references.
    pub const AGENT_REFERENCES: &str = "agent_references";
    /// Connected-account references.
    pub const CONNECTIONS: &str = "connections";
    /// Tool catalog contracts.
    pub const TOOL_CONTRACTS: &str = "tool_contracts";
    /// Scope-bound output contract lookup for advisory wiring diagnostics.
    pub const LOOKUP_TOOL_CONTRACT: &str = "LookupToolContract";
}

/// Stable authoring member names.
pub mod methods {
    /// Migrate, parse, structurally validate and check engine compatibility.
    pub const VALIDATE_GRAPH: &str = "ValidateGraph";
    /// Normalize native or n8n input and validate it without persistence.
    pub const IMPORT_GRAPH: &str = "ImportGraph";
    /// Check bindings that omit the required node-output envelope.
    pub const BINDING_FAILURES: &str = "BindingFailures";
    /// Check portable binding, agent prompt/schema, and code-language gates.
    pub const AUTHORING_FAILURES: &str = "AuthoringFailures";
    /// Mock-run outbound arguments, skipping host-provided native slug prefixes.
    pub const UNRESOLVABLE_TOOL_ARGS: &str = "UnresolvableToolArgs";
    /// Ordered engine and host gates followed by mock argument preflight.
    pub const BUILDER_FAILURES: &str = "BuilderFailures";
    /// Best-effort generic output, envelope, and split path diagnostics.
    pub const WIRING_WARNINGS: &str = "WiringWarnings";
}

/// Host-published output contract for one tool, used only for advisory checks.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct ToolContract {
    /// Published top-level payload fields, when known.
    pub output_fields: Vec<String>,
    /// Payload JSON schema after removing `envelope_prefix`, when known.
    pub output_schema: Option<Value>,
    /// Dotted path of the primary array relative to the full output, including
    /// any envelope prefix. The empty string denotes a root array.
    pub primary_array_path: Option<String>,
    /// Payload envelope prefix, such as `data` or `payload`; absent or empty
    /// means the schema is unwrapped.
    pub envelope_prefix: Option<String>,
    /// Top-level metadata fields outside the payload envelope.
    pub metadata_fields: Vec<String>,
}

/// Graph and scope-bound host lookup for the advisory wiring operation.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct WiringWarningsRequest {
    /// Candidate workflow graph JSON.
    pub graph: Value,
    /// Host callback scoped to this author's catalog and identity.
    pub host: AuthoringHost,
}

/// Request to migrate, validate, and check compatibility of a candidate graph.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct ValidateGraphRequest {
    /// Candidate graph JSON.
    pub graph: Value,
    /// Optional scope-bound saved-workflow resolver.
    pub host: Option<AuthoringHost>,
}

/// Request to import and validate a graph without persisting it.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct ImportGraphRequest {
    /// Source document.
    pub graph: Value,
    /// `native`/`tinyflows`, `n8n`, or `auto`; names are trimmed and case-insensitive.
    pub format: Option<String>,
}

/// Result of best-effort import and engine validation.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct ImportGraphResult {
    /// Migrated graph, present when parsing succeeded.
    pub graph: Option<Value>,
    /// Non-fatal importer warnings.
    pub warnings: Vec<String>,
    /// Structural and engine compatibility failures.
    pub errors: Vec<ValidationError>,
}

/// One structured graph validation failure.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ValidationError {
    /// Stable failure classification.
    pub code: String,
    /// Author-facing explanation supplied by the engine.
    pub message: String,
    /// Offending node, when known.
    pub node_id: Option<String>,
    /// Offending field, when known.
    pub field: Option<String>,
}

/// Result of migration and graph validation without persistence or host I/O.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct GraphValidation {
    /// Migrated graph when parsing succeeded, including invalid graphs so an
    /// editor can keep the author's work while presenting errors.
    pub graph: Option<Value>,
    /// Every failure from the first failing stage (parse, structure, topology).
    pub errors: Vec<ValidationError>,
}

impl GraphValidation {
    /// Whether all engine validation stages succeeded.
    #[must_use]
    pub fn is_valid(&self) -> bool {
        self.graph.is_some() && self.errors.is_empty()
    }
}

pub mod catalog;
/// Transport-free node-kind authoring contracts.
pub mod node_kinds;
pub mod runs;
pub mod schedule;

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;
