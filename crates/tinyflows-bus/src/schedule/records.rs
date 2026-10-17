//! Plain-data records the host callbacks carry. A host implementing the
//! `ScheduleHost` callbacks decodes and encodes these instead of hand-rolled
//! JSON; the JSON shapes are the wire contract.
use super::types::DeliveryStatus;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Host-classified result of one attempt. Permanent failures halt immediately.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Attempt {
    /// Whether execution (or final execution plus delivery) succeeded.
    pub success: bool,
    /// Result text passed unchanged to delivery and run-history persistence.
    pub output: String,
    /// False for host-classified permanent failures; stops retry immediately.
    pub retryable: bool,
    /// Opaque host-classification detail. The module retains the last
    /// non-empty value across retries and relays it unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_detail: Option<String>,
}

/// Persisted outcome after execution and delivery.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunRecord {
    /// Stable invocation identity shared by all attempts and delivery.
    pub run_id: String,
    /// Identity of the job that produced this run.
    pub job_id: String,
    /// Dispatch-time creation identity. Stores must reject a replacement with
    /// the same job ID instead of attaching this outcome to its history.
    pub job_created_at: DateTime<Utc>,
    /// Execution start, before the first attempt.
    pub started_at: DateTime<Utc>,
    /// Execution finish before delivery; skipped runs use the same start/end.
    pub finished_at: DateTime<Utc>,
    /// `ok`, `error`, or `skipped`.
    pub status: String,
    /// Result text passed unchanged to delivery and run-history persistence.
    pub output: String,
    /// Delivery outcome, absent on skipped slots.
    pub delivery_status: Option<DeliveryStatus>,
}

/// Final mutation computed by the runtime. Stores apply it mechanically.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Completion {
    /// Whether a scheduled run advances or disables its saved occurrence.
    /// Manual Run Now keeps the schedule untouched.
    #[serde(default = "advance_schedule_default")]
    pub advance_schedule: bool,
    /// Remove a successful auto-delete one-shot, including its stored history,
    /// unless a concurrent edit re-enabled or otherwise retained it.
    pub remove: bool,
    /// Retain a one-shot already disabled at claim time. A store must preserve
    /// an explicit re-enable made after that claim.
    pub disable: bool,
    /// Recurrence computed after delivery; absent for one-shot jobs.
    pub next_run: Option<DateTime<Utc>>,
    /// Execution finish before delivery; skipped runs use the same start/end.
    pub finished_at: DateTime<Utc>,
    /// Whether execution (or final execution plus delivery) succeeded.
    pub success: bool,
    /// Result text passed unchanged to delivery and run-history persistence.
    pub output: String,
}

fn advance_schedule_default() -> bool {
    true
}
