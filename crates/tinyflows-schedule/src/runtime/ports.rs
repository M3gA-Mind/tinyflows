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
    pub success: bool,
    pub output: String,
    pub retryable: bool,
}

/// Persisted outcome after execution and delivery.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunRecord {
    pub run_id: String,
    pub job_id: String,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub status: String,
    pub output: String,
    pub delivery_status: Option<DeliveryStatus>,
}

/// Final mutation computed by the runtime. Stores apply it mechanically.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Completion {
    pub remove: bool,
    pub disable: bool,
    pub next_run: Option<DateTime<Utc>>,
    pub finished_at: DateTime<Utc>,
    pub success: bool,
    pub output: String,
}

/// Storage bound to one agent/tenant scope.
#[async_trait]
pub trait JobStore: Send + Sync {
    async fn due_jobs(&self, now: DateTime<Utc>) -> Result<Vec<CronJob>>;
    async fn claim_slot(&self, job: &CronJob, next_run: DateTime<Utc>) -> Result<()>;
    async fn record_run(&self, record: RunRecord) -> Result<()>;
    async fn complete(&self, job: &CronJob, completion: Completion) -> Result<()>;
}

/// Agent and shell execution, including host security and failure classification.
#[async_trait]
pub trait JobExecutor: Send + Sync {
    async fn execute(&self, job: &CronJob, run_id: &str) -> Result<Attempt>;
    /// Product reporting after a final failed attempt; never on a retry.
    async fn failed(&self, _job: &CronJob, _run_id: &str, _attempt: &Attempt) {}
}

/// Product delivery to channels, conversation origins and alerts.
#[async_trait]
pub trait RunDelivery: Send + Sync {
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
    async fn schedule(&self, flow_id: &str, run_id: &str) -> Result<Attempt>;
}

/// Injectable wall clock and wait primitive. Tests advance time without sleeping.
#[async_trait]
pub trait Clock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
    async fn sleep(&self, duration: Duration);
}

/// Production wall clock.
pub struct SystemClock;
#[async_trait]
impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
    async fn sleep(&self, duration: Duration) {
        tokio::time::sleep(duration).await;
    }
}
