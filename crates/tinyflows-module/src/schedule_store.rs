//! Module-owned persistence dispatch. Hosts pass configuration and JSON only.
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde_json::Value;
use std::sync::Arc;
use tinybus::Connection;
use tinyflows_bus::schedule::{
    StoreBackend, StoreConfig, StoreOperation as Operation, StoreRequest,
};
use tinyflows_drivers::schedule::CronDocuments;
use tinyflows_schedule::CronJob;

/// The module's schedule store: `CronDocuments` over the host's document port.
pub(super) struct Store(CronDocuments);
impl Store {
    pub(super) async fn new(connection: Connection, config: StoreConfig) -> Result<Self> {
        let StoreBackend::Documents { callback } = config.backend;
        Ok(Self(
            CronDocuments::new(Arc::new(
                super::schedule_documents::Documents::new(connection, callback).await?,
            ))
            .with_limits(config.max_run_history, config.max_tasks),
        ))
    }
    pub(super) async fn request(connection: Connection, request: StoreRequest) -> Result<Value> {
        Self::new(connection, request.storage)
            .await?
            .execute(serde_json::from_value(request.operation)?)
            .await
    }
    async fn execute(&self, operation: Operation) -> Result<Value> {
        // The store owns validation, migration, output limits and history pruning.
        let documents = &self.0;
        macro_rules! dispatch {
            ($method:ident $(, $arg:expr)*) => {
                serde_json::to_value(documents.$method($($arg),*).await?)?
            };
        }
        Ok(match operation {
            Operation::Add {
                expression,
                command,
            } => dispatch!(add_job, &expression, &command),
            Operation::AddShell {
                name,
                schedule,
                command,
            } => dispatch!(add_shell_job, name, schedule, &command),
            Operation::AddAgent { spec } => dispatch!(add_agent_job_from_spec, spec),
            Operation::AddFlow { flow_id, schedule } => {
                dispatch!(add_flow_schedule_job, &flow_id, schedule)
            }
            Operation::FindFlow { flow_id } => dispatch!(find_flow_schedule_job, &flow_id),
            Operation::List => dispatch!(list_jobs),
            Operation::Get { job_id } => dispatch!(get_job, &job_id),
            Operation::Remove { job_id } => dispatch!(remove_job, &job_id),
            Operation::Clear => dispatch!(clear_all_jobs),
            Operation::Dedup => dispatch!(dedup_named_jobs),
            Operation::Due { now } => dispatch!(due_jobs, now),
            Operation::Update { job_id, patch } => dispatch!(update_job, &job_id, patch),
            Operation::RecordLastRun {
                job_id,
                finished_at,
                success,
                output,
            } => dispatch!(record_last_run, &job_id, finished_at, success, &output),
            Operation::Reschedule {
                job,
                success,
                output,
            } => dispatch!(reschedule_after_run, &job, success, &output),
            Operation::RecordRun {
                job_id,
                started_at,
                finished_at,
                status,
                output,
                duration_ms,
                delivery_status,
            } => dispatch!(
                record_run_with_delivery,
                &job_id,
                started_at,
                finished_at,
                &status,
                output.as_deref(),
                duration_ms,
                delivery_status
            ),
            Operation::DeleteQueuedRuns { job_id } => dispatch!(delete_queued_runs, &job_id),
            Operation::ListRuns { job_id, limit } => dispatch!(list_runs, &job_id, limit),
            Operation::SetMaxActiveJobs { limit } => dispatch!(set_max_active_jobs, limit),
            Operation::ImportJob { job } => dispatch!(import_job, &job),
            Operation::ImportRun { run } => dispatch!(import_run, &run),
        })
    }
}

#[async_trait::async_trait]
impl tinyflows_schedule::runtime::JobStore for Store {
    async fn due_jobs(&self, now: DateTime<Utc>) -> Result<Vec<CronJob>> {
        self.0.due_jobs(now).await
    }
    async fn due_jobs_excluding(
        &self,
        now: DateTime<Utc>,
        excluded: &[String],
    ) -> Result<Vec<CronJob>> {
        self.0.due_jobs_excluding(now, excluded).await
    }
    async fn claim_slot(&self, job: &CronJob, next: DateTime<Utc>) -> Result<()> {
        let claimed = self.0.claim_slot(job, next).await?;
        anyhow::ensure!(
            claimed,
            "schedule occurrence was removed, edited or already claimed"
        );
        Ok(())
    }
    async fn record_run(&self, record: tinyflows_schedule::runtime::RunRecord) -> Result<()> {
        self.0.record_claimed_run(&record).await
    }

    async fn complete(
        &self,
        job: &CronJob,
        completion: tinyflows_schedule::runtime::Completion,
    ) -> Result<()> {
        self.0.complete_run(job, &completion).await
    }
}

#[cfg(test)]
#[path = "schedule_store_tests.rs"]
mod tests;
