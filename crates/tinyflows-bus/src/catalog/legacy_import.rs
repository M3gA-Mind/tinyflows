//! Transport-free legacy flow and checkpoint import batches.

use super::CatalogStorage;
use serde::{Deserialize, Serialize};

/// Maximum source records accepted in one Catalog import call.
pub const MAX_IMPORT_BATCH_RECORDS: usize = 64;
/// Maximum serialized request size accepted by Catalog for one import call.
pub const MAX_IMPORT_BATCH_BYTES: usize = 2 * 1024 * 1024;

/// The legacy record family a problem or tally refers to.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LegacyImportKind {
    /// A saved workflow definition.
    #[serde(rename = "flows")]
    Flow,
    /// A saved workflow revision.
    Revision,
    /// A previous workflow run.
    Run,
    /// A workflow state key/value.
    State,
    /// A discovered workflow suggestion.
    Suggestion,
    /// A builder draft.
    Draft,
    /// A graph checkpoint.
    Checkpoint,
    /// Pending writes attached to a graph checkpoint.
    Write,
}

/// A definition row as stored by the retired SQLite flow catalog.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct LegacyFlowRecord {
    /// The historical workflow id.
    pub id: String,
    /// Display name.
    pub name: String,
    /// Raw JSON from `graph_json`; TinyFlows performs graph migration and decode.
    pub graph_json: String,
    /// Historical enablement flag.
    pub enabled: bool,
    /// RFC3339 creation time.
    pub created_at: String,
    /// RFC3339 last update time.
    pub updated_at: String,
    /// RFC3339 time of the latest run, when present.
    pub last_run_at: Option<String>,
    /// Latest run status, when present.
    pub last_status: Option<String>,
    /// Historical approval preference.
    pub require_approval: bool,
}

/// A revision row as stored by the retired SQLite flow catalog.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct LegacyRevisionRecord {
    /// Historical revision id.
    pub id: String,
    /// Historical owner flow id.
    pub flow_id: String,
    /// Raw JSON from `graph_json`; revisions preserve the graph as stored.
    pub graph_json: String,
    /// Name at the time the revision was captured.
    pub name: String,
    /// Approval preference at the time the revision was captured.
    pub require_approval: bool,
    /// RFC3339 capture time.
    pub created_at: String,
}

/// A run row as stored by the retired SQLite flow catalog.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct LegacyRunRecord {
    /// Historical run id.
    pub id: String,
    /// Historical owner flow id.
    pub flow_id: String,
    /// Historical checkpointer thread id.
    pub thread_id: String,
    /// Run state, including parked and still-live values.
    pub status: String,
    /// RFC3339 start time.
    pub started_at: String,
    /// RFC3339 finish time, when present.
    pub finished_at: Option<String>,
    /// Raw JSON from `steps_json`.
    pub steps_json: String,
    /// Raw JSON from `pending_approvals_json`.
    pub pending_approvals_json: String,
    /// Failure detail, when present.
    pub error: Option<String>,
    /// Approval freshness hash, when present in this legacy schema.
    pub graph_hash: Option<String>,
}

/// A state row as stored by the retired SQLite flow catalog.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct LegacyStateRecord {
    /// Application state namespace.
    pub namespace: String,
    /// State item id.
    pub key: String,
    /// Raw JSON from the legacy value column.
    pub value_json: String,
}

/// A suggestion row as stored by the retired SQLite flow catalog.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct LegacySuggestionRecord {
    /// Historical suggestion id.
    pub id: String,
    /// Display title.
    pub title: String,
    /// Short description.
    pub one_liner: String,
    /// Why the suggestion may help.
    pub rationale: String,
    /// Trigger hint, when present.
    pub trigger_hint: Option<String>,
    /// Raw JSON from the list columns.
    pub steps_json: String,
    /// Raw JSON from the list columns.
    pub connections_json: String,
    /// Raw JSON from the list columns.
    pub slugs_json: String,
    /// Builder prompt.
    pub build_prompt: String,
    /// Historical confidence.
    pub confidence: f64,
    /// Historical status string; unknown values retain the old lossy mapping.
    pub status: String,
    /// RFC3339 creation time.
    pub created_at: String,
    /// Source run id, when present.
    pub source_run_id: Option<String>,
}

/// Raw bytes from one non-staging JSON draft file.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct LegacyDraftRecord {
    /// File name only; no host path crosses the module boundary.
    pub file_name: String,
    /// Original file bytes; TinyFlows decodes the draft JSON.
    pub bytes: Vec<u8>,
}

/// A checkpoint write stored in `checkpoints.db`.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct LegacySqliteCheckpointWrite {
    /// Checkpointer thread id.
    pub thread_id: String,
    /// Raw JSON array of subgraph namespace components.
    pub namespace_json: String,
    /// Checkpoint id.
    pub checkpoint_id: String,
    /// Task id.
    pub task_id: String,
    /// Task-local write index.
    pub idx: i64,
    /// Node id.
    pub node: String,
    /// Channel name.
    pub channel: String,
    /// Raw JSON payload.
    pub payload_json: String,
}

/// One source record sent to the scoped Catalog import member.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "kind", content = "record", rename_all = "snake_case")]
pub enum LegacyImportRecord {
    /// Historical workflow definition.
    Flow(LegacyFlowRecord),
    /// Historical workflow revision.
    Revision(LegacyRevisionRecord),
    /// Historical run.
    Run(LegacyRunRecord),
    /// Historical flow state.
    State(LegacyStateRecord),
    /// Historical workflow suggestion.
    Suggestion(LegacySuggestionRecord),
    /// Raw builder-draft JSON file.
    Draft(LegacyDraftRecord),
    /// Raw serialized graph checkpoint. TinyFlows owns typed checkpoint decode.
    Checkpoint { raw_json: String },
    /// Pending write row from `checkpoints.db`.
    SqliteCheckpointWrite(LegacySqliteCheckpointWrite),
    /// Raw pending-write JSONL line with its decoded file thread id.
    FileCheckpointWrite { thread_id: String, raw_json: String },
    /// A source row too large to send or a bounded source-read problem.
    /// The identity and reason are retained without carrying the rejected body.
    Invalid {
        /// Record family.
        record_kind: LegacyImportKind,
        /// Safe record identity, when one could be read.
        identity: Option<String>,
        /// Bounded reason for the skipped item.
        message: String,
    },
}

/// A bounded write-if-absent batch for the host-selected catalog scope.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ImportLegacyBatchRequest {
    /// The host's already-scoped catalog document callback.
    pub storage: CatalogStorage,
    /// Ordered records. Callers must not exceed [`MAX_IMPORT_BATCH_RECORDS`].
    pub records: Vec<LegacyImportRecord>,
}

/// Per-kind result for one legacy import batch.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct LegacyImportTally {
    /// Legacy record family.
    pub kind: LegacyImportKind,
    /// Records written in this batch.
    pub written: u64,
    /// Records already present and left untouched.
    pub existing: u64,
    /// Records that could not be decoded or whose dependency was absent.
    pub invalid: u64,
}

/// A bounded per-record issue returned by the module.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct LegacyImportProblem {
    /// Legacy record family.
    pub kind: LegacyImportKind,
    /// Record id or file name, when known.
    pub identity: Option<String>,
    /// Safe explanation; never contains the rejected record body.
    pub message: String,
}

/// Bounded import report for one batch.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
pub struct ImportLegacyBatchResponse {
    /// Per-kind tallies from this batch.
    pub tallies: Vec<LegacyImportTally>,
    /// Problems from this batch, bounded by its record count.
    pub problems: Vec<LegacyImportProblem>,
}

/// Returns the kind carried by this record, for batching and tally validation.
impl LegacyImportRecord {
    /// The tally family for this source record.
    #[must_use]
    pub fn record_kind(&self) -> LegacyImportKind {
        match self {
            Self::Flow(_) => LegacyImportKind::Flow,
            Self::Revision(_) => LegacyImportKind::Revision,
            Self::Run(_) => LegacyImportKind::Run,
            Self::State(_) => LegacyImportKind::State,
            Self::Suggestion(_) => LegacyImportKind::Suggestion,
            Self::Draft(_) => LegacyImportKind::Draft,
            Self::Checkpoint { .. } => LegacyImportKind::Checkpoint,
            Self::SqliteCheckpointWrite(_) | Self::FileCheckpointWrite { .. } => {
                LegacyImportKind::Write
            }
            Self::Invalid { record_kind, .. } => *record_kind,
        }
    }
}
