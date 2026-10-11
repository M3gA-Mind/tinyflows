//! Runtime seams: the scheduler owns timing and bookkeeping; hosts own I/O.
use crate::{CronJob, DeliveryStatus};
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Host-classified result of one attempt. Permanent failures halt immediately.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Attempt {
    /// Whether execution (or final execution plus delivery) succeeded.
    pub success: bool,
    /// Result text passed unchanged to delivery and run-history persistence.
    pub output: String,
    /// False for host-classified permanent failures; stops retry immediately.
    pub retryable: bool,
}

/// Persisted outcome after execution and delivery.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunRecord {
    /// Stable invocation identity shared by all attempts and delivery.
    pub run_id: String,
    /// Identity of the job that produced this run.
    pub job_id: String,
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
    /// Remove a successful auto-delete one-shot, including its stored history.
    pub remove: bool,
    /// Retain but disable a one-shot that was not successfully auto-deleted.
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

/// Storage bound to one agent/tenant scope.
#[async_trait]
pub trait JobStore: Send + Sync {
    /// Read enabled jobs due at `now` within the store's configured batch limit.
    async fn due_jobs(&self, now: DateTime<Utc>) -> Result<Vec<CronJob>>;
    /// Durably claim this occurrence before dispatch. Preserve concurrent edits.
    /// Return an error on storage failure or when another writer won the claim.
    async fn claim_slot(&self, job: &CronJob, next_run: DateTime<Utc>) -> Result<()>;
    /// Append a run outcome without changing the job's schedule.
    async fn record_run(&self, record: RunRecord) -> Result<()>;
    /// Apply the computed terminal mutation and last-run fields.
    async fn complete(&self, job: &CronJob, completion: Completion) -> Result<()>;
}

/// Agent and shell execution, including host security and failure classification.
#[async_trait]
pub trait JobExecutor: Send + Sync {
    /// Execute one attempt, enforcing the host's policy under the bound scope.
    async fn execute(&self, job: &CronJob, run_id: &str) -> Result<Attempt>;
    /// Product reporting after a final failed attempt; never on a retry.
    async fn failed(&self, _job: &CronJob, _run_id: &str, _attempt: &Attempt) {}
}

/// Product delivery to channels, conversation origins and alerts.
#[async_trait]
pub trait RunDelivery: Send + Sync {
    /// Deliver one final attempt; an error may fail required delivery.
    async fn deliver(
        &self,
        job: &CronJob,
        run_id: &str,
        attempt: &Attempt,
    ) -> Result<DeliveryStatus>;
}

/// Direct schedule-trigger dispatch; no host domain-event round trip.
#[async_trait]
pub trait TriggerSink: Send + Sync {
    /// Fire the bound flow's schedule trigger once for this attempt.
    async fn schedule(&self, flow_id: &str, run_id: &str) -> Result<Attempt>;
}

/// Injectable wall clock and wait primitive. Tests advance time without sleeping.
#[async_trait]
pub trait Clock: Send + Sync {
    /// Current wall-clock instant used for due selection, jitter and recurrence.
    fn now(&self) -> DateTime<Utc>;
    /// Wait for a poll interval or retry delay; test clocks may advance directly.
    async fn sleep(&self, duration: Duration);
}

/// Production wall clock.
pub struct SystemClock;
#[async_trait]
impl Clock for SystemClock {
    /// Current wall-clock instant used for due selection, jitter and recurrence.
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
    /// Wait for a poll interval or retry delay; test clocks may advance directly.
    async fn sleep(&self, duration: Duration) {
        tokio::time::sleep(duration).await;
    }
}
