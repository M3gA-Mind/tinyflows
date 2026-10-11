//! Transport-only scheduler vocabulary. Jobs and records keep their JSON wire
//! shape without making hosts link scheduler or storage implementation crates.
use serde::{Deserialize, Serialize};

/// The module has one bus identity and separately served authoring/schedule objects.
pub const BUS_NAME: &str = crate::BUS_NAME;
pub const INTERFACE: &str = "ai.tinyhumans.tinyflows.Schedule";
pub const OBJECT_PATH: &str = "/ai/tinyhumans/tinyflows/Schedule";
pub const CALLBACK_INTERFACE: &str = "ai.tinyhumans.tinyflows.ScheduleHost";
pub const CONTRACT_VERSION: u32 = 1;
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
    pub destination: String,
    pub object_path: String,
}

/// Whether the module runs its poll loop, or the host drives individual ticks.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StartScheduler {
    pub scope: String,
    pub callback: CallbackAddress,
    pub config: serde_json::Value,
    #[serde(default)]
    pub manual_ticks: bool,
}

/// Stable host callback member names. The host owns execution policy and I/O;
/// the module computes claims, retry timing, completion and rescheduling.
pub mod callbacks {
    pub const DUE_JOBS: &str = "DueJobs";
    pub const CLAIM_SLOT: &str = "ClaimSlot";
    pub const RECORD_RUN: &str = "RecordRun";
    pub const COMPLETE: &str = "Complete";
    pub const EXECUTE: &str = "Execute";
    pub const FAILED: &str = "Failed";
    pub const DELIVER: &str = "Deliver";
    pub const SCHEDULE_TRIGGER: &str = "ScheduleTrigger";
}
