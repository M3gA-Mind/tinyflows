//! Runtime seams: the scheduler owns timing and bookkeeping; hosts own I/O.
use crate::{CronJob, DeliveryStatus};
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::time::Duration;

pub use tinyflows_bus::schedule::{Attempt, Completion, RunRecord};

/// Storage bound to one agent/tenant scope.
#[async_trait]
pub trait JobStore: Send + Sync {
    /// Read enabled jobs due at `now` within the store's configured batch limit.
    async fn due_jobs(&self, now: DateTime<Utc>) -> Result<Vec<CronJob>>;
    /// Read due jobs after excluding examined identities, before applying the
    /// configured batch limit. Bounded stores must override this operation;
    /// its default fails closed instead of silently starving later jobs.
    async fn due_jobs_excluding(
        &self,
        now: DateTime<Utc>,
        excluded: &[String],
    ) -> Result<Vec<CronJob>> {
        anyhow::ensure!(
            excluded.is_empty(),
            "job store does not support due-job exclusions"
        );
        self.due_jobs(now).await
    }
    /// Durably claim this occurrence before dispatch. Preserve concurrent edits.
    /// Compare job.claim_generation and atomically increment it. One-shots
    /// must be durably disabled before returning success.
    /// Return an error on storage failure or when another writer won the claim.
    async fn claim_slot(&self, job: &CronJob, next_run: DateTime<Utc>) -> Result<()>;
    /// Append a run outcome without changing the job's schedule. Match its
    /// dispatch-time job_created_at; reject a replacement with the same ID.
    async fn record_run(&self, record: RunRecord) -> Result<()>;
    /// Apply the computed terminal mutation and last-run fields after a claim,
    /// preserving explicit re-enables and edits made while the run executed.
    /// Mutate only when the current generation equals job.claim_generation + 1;
    /// a later successful claim makes this completion stale.
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
