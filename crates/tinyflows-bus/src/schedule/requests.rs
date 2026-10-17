//! Request types for the pure schedule-maths and validation members. The
//! algorithms (cron parsing, time zones, active hours, cadence floor) execute
//! in the module; hosts only send these and read the replies.
use super::types::{
    CronJob, CronJobPatch, DeliveryConfig, DeliveryStatus, JobOrigin, Schedule, SessionTarget,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Module-owned manual admission. An accepted request is durably queued before
/// the reply; `wait` selects a blocking tool call or a detached RPC response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunJobNow {
    /// The scheduler/store scope, bound by the caller.
    pub scope: String,
    /// Context-bound host execution and delivery callbacks.
    pub callback: super::CallbackAddress,
    /// Retry and timing configuration.
    pub config: serde_json::Value,
    /// Saved snapshot to execute, even when disabled or scheduled in the future.
    pub job: CronJob,
    /// Stable identity chosen by the module or caller for this accepted run.
    pub run_id: String,
    /// Do not send a delivery into the current cron tool turn.
    #[serde(default)]
    pub delivery: ManualDelivery,
    /// Wait for terminal persistence; false returns after durable queueing.
    #[serde(default)]
    pub wait: bool,
}

/// Whether a manual run sends its normal product delivery.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ManualDelivery {
    /// Send through the context-bound host callback.
    #[default]
    Requested,
    /// The caller is already the response channel (for example CronRunTool).
    NotRequested,
}

/// Typed result of module-owned manual admission.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RunJobReply {
    /// The durable queue row and occurrence claim exist.
    Accepted { run_id: String },
    /// A blocking call completed with its terminal outcome.
    Completed {
        run_id: String,
        attempt: super::Attempt,
        success: bool,
        delivery_status: Option<DeliveryStatus>,
    },
    /// Admission refused without pretending that a run was queued.
    Refused { reason: RunJobRefusal },
}

/// Machine-readable manual admission refusal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunJobRefusal {
    /// Another scheduled or manual run currently owns the scoped job claim.
    AlreadyRunning,
    /// The saved definition was removed, replaced, or edited before claim.
    DefinitionChanged,
}

/// Receipt returned only by the scoped storage callback after an atomic claim.
/// `receipt` is opaque to the model and may only be passed back to lease calls.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OccurrenceClaim {
    /// Storage-owned fencing receipt.
    pub receipt: String,
    /// Exact definition snapshot captured by the successful claim.
    pub job: CronJob,
    /// Scheduled occurrence instant captured at acquisition.
    pub occurrence: DateTime<Utc>,
    /// Owner chosen by the module for renewal and fencing.
    pub owner: String,
}

/// Host's product-side terminal projection after send and final success are known.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryProjection {
    /// Captured definition used for this occurrence.
    pub job: CronJob,
    /// Stable invocation identity.
    pub run_id: String,
    /// Final host-classified attempt, including retained raw detail.
    pub attempt: super::Attempt,
    /// Runtime decision after actual send outcome and best-effort policy.
    pub success: bool,
    /// Actual result of the send phase; absent when delivery was not requested.
    pub delivery_status: Option<DeliveryStatus>,
}

/// `NextRun`: the first occurrence of `schedule` strictly after `from`.
/// Reply: `DateTime<Utc>` (RFC3339). Errors on an unparseable schedule.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NextRunRequest {
    /// Schedule to evaluate.
    pub schedule: Schedule,
    /// Instant to search from.
    pub from: DateTime<Utc>,
}

/// `ValidateSchedule`: reject a schedule that can never fire, or (when
/// `agent` is set) one that runs more often than `MIN_AGENT_JOB_INTERVAL`.
/// Reply: unit; the error message is the author-facing explanation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidateScheduleRequest {
    /// Schedule to validate.
    pub schedule: Schedule,
    /// Reference instant (one-shots must lie after it).
    pub now: DateTime<Utc>,
    /// Apply the agent-job cadence floor as well.
    #[serde(default)]
    pub agent: bool,
}

/// `RunsCloserThan`: the closest pair of consecutive runs after `from` that
/// is under `min_gap_ms` apart. Reply: `Option<TooFrequent>`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunsCloserThanRequest {
    /// Schedule to inspect.
    pub schedule: Schedule,
    /// Instant the walk starts from.
    pub from: DateTime<Utc>,
    /// Threshold in milliseconds.
    pub min_gap_ms: i64,
}

/// `CheckOriginRequirements`: refuse origin delivery or a `current` session
/// target when the job has no origin conversation. Reply: unit; the error
/// message is the author-facing explanation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckOriginRequest {
    /// Requested session target.
    pub session_target: SessionTarget,
    /// Requested delivery.
    pub delivery: DeliveryConfig,
    /// The creating conversation, when there is one.
    #[serde(default)]
    pub origin: Option<JobOrigin>,
}

/// `CheckPatch`: refuse a patch no store may apply to `job` (re-targeting a
/// flow-schedule job). Reply: unit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckPatchRequest {
    /// The stored job.
    pub job: CronJob,
    /// The proposed patch.
    pub patch: CronJobPatch,
}
