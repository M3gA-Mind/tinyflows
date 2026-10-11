//! Transport-only scheduler vocabulary. Jobs and records keep their JSON wire
//! shape without making hosts link scheduler or storage implementation crates.
use serde::{Deserialize, Serialize};

/// The module has one bus identity and separately served authoring/schedule objects.
pub const BUS_NAME: &str = crate::BUS_NAME;
/// Separate scheduler interface in the shared module.
pub const INTERFACE: &str = "ai.tinyhumans.tinyflows.Schedule";
/// Served scheduler object path.
pub const OBJECT_PATH: &str = "/ai/tinyhumans/tinyflows/Schedule";
/// Host interface carrying the context-bound runtime ports.
pub const CALLBACK_INTERFACE: &str = "ai.tinyhumans.tinyflows.ScheduleHost";
/// Version of the scheduler wire vocabulary.
pub const CONTRACT_VERSION: u32 = 1;
/// Exact advertised scheduler member set.
pub const METHODS: &[&str] = &[
    "StartScheduler",
    "StopScheduler",
    "TickScheduler",
    "DrainScheduler",
    "ExecuteJob",
    "ParseHumanDelay",
    "ValidateDelivery",
];

/// A host callback object bound to the owning agent/tenant context.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CallbackAddress {
    /// Bus name of the host callback service.
    pub destination: String,
    /// Callback object bound to one agent/tenant context.
    pub object_path: String,
}

/// Whether the module runs its poll loop, or the host drives individual ticks.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StartScheduler {
    /// Process-wide claim namespace; equal keys share job and loop claims.
    pub scope: String,
    /// Context-bound I/O callbacks used for every scheduled run.
    pub callback: CallbackAddress,
    /// `poll_secs`, `max_concurrent`, `retries`, `backoff_ms`; omitted keys use defaults.
    pub config: serde_json::Value,
    #[serde(default)]
    /// True to drive ticks/drain through calls; false starts the module-owned loop.
    pub manual_ticks: bool,
}

/// Stable host callback member names. The host owns execution policy and I/O;
/// the module computes claims, retry timing, completion and rescheduling.
pub mod callbacks {
    /// `(now: RFC3339) -> jobs: JSON[]`.
    pub const DUE_JOBS: &str = "DueJobs";
    /// `(job: JSON, next_run: RFC3339) -> ()`, preserving concurrent edits.
    pub const CLAIM_SLOT: &str = "ClaimSlot";
    /// `(record: JSON) -> ()`; fields match runtime run records.
    pub const RECORD_RUN: &str = "RecordRun";
    /// `(job: JSON, completion: JSON) -> ()`; apply the computed mutation.
    pub const COMPLETE: &str = "Complete";
    /// `(job: JSON, run_id: string) -> {success, output, retryable}`.
    pub const EXECUTE: &str = "Execute";
    /// `(job: JSON, run_id: string, attempt: JSON) -> ()` after final failure.
    pub const FAILED: &str = "Failed";
    /// `(job: JSON, run_id: string, attempt: JSON) -> delivery-status string.
    pub const DELIVER: &str = "Deliver";
    /// `(flow_id: string, run_id: string) -> {success, output, retryable}`.
    pub const SCHEDULE_TRIGGER: &str = "ScheduleTrigger";
}
