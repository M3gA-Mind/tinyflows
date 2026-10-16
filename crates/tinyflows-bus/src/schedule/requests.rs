//! Request types for the pure schedule-maths and validation members. The
//! algorithms (cron parsing, time zones, active hours, cadence floor) execute
//! in the module; hosts only send these and read the replies.
use super::types::{CronJob, CronJobPatch, DeliveryConfig, JobOrigin, Schedule, SessionTarget};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

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
