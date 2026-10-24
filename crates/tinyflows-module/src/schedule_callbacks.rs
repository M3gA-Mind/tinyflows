//! Adapt a context-bound TinyBus host callback object to scheduler ports.
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Serialize, de::DeserializeOwned};
use std::sync::Arc;
use tinybus::Connection;
use tinyflows_bus::schedule::{
    CALLBACK_INTERFACE, CallbackAddress, ManualClaimReply, OccurrenceClaim, RunJobRefusal,
    callbacks,
};
use tinyflows_schedule::{CronJob, DeliveryStatus, runtime::*};

/// How long the host may take to run, report, deliver or trigger one job.
pub(super) const RUN_CALLBACK_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(2 * 60 * 60);

pub(super) struct HostCallbacks {
    connection: Connection,
    address: CallbackAddress,
}
impl HostCallbacks {
    pub(super) fn ports(connection: Connection, address: CallbackAddress) -> RuntimePorts {
        let host = Arc::new(Self {
            connection,
            address,
        });
        RuntimePorts {
            store: host.clone(),
            executor: host.clone(),
            delivery: host.clone(),
            triggers: host,
            clock: Arc::new(SystemClock),
        }
    }
    async fn call<A: Serialize + Send + Sync, R: DeserializeOwned>(
        &self,
        member: &str,
        args: &A,
    ) -> Result<R> {
        let mut proxy = self.connection.proxy(
            &self.address.destination,
            &self.address.object_path,
            CALLBACK_INTERFACE,
        )?;
        // Running, failing, delivering and triggering are the host doing the
        // job's actual work (an agent turn, a shell command, a flow): the bus
        // default of 30 seconds would cut every long run short and make the
        // runtime retry work that is still going.
        if matches!(
            member,
            callbacks::EXECUTE
                | callbacks::FAILED
                | callbacks::SEND
                | callbacks::DELIVER
                | callbacks::SCHEDULE_TRIGGER
        ) {
            proxy = proxy.with_timeout(RUN_CALLBACK_TIMEOUT);
        }
        Ok(proxy.call(member, args).await?)
    }
}
#[async_trait]
impl JobStore for HostCallbacks {
    async fn due_jobs(&self, now: DateTime<Utc>) -> Result<Vec<CronJob>> {
        self.call(callbacks::DUE_JOBS, &(now,)).await
    }
    async fn due_jobs_excluding(
        &self,
        now: DateTime<Utc>,
        excluded: &[String],
    ) -> Result<Vec<CronJob>> {
        self.call(callbacks::DUE_JOBS_EXCLUDING, &(now, excluded))
            .await
    }
    async fn claim_due_occurrence(
        &self,
        job: &CronJob,
        owner: &str,
        now: DateTime<Utc>,
        lease: chrono::Duration,
    ) -> Result<Option<OccurrenceClaim>> {
        self.call(
            callbacks::CLAIM_DUE_OCCURRENCE,
            &(job, owner, now, lease.num_milliseconds()),
        )
        .await
    }
    async fn claim_manual_occurrence(
        &self,
        job: &CronJob,
        owner: &str,
        now: DateTime<Utc>,
        lease: chrono::Duration,
    ) -> Result<Result<OccurrenceClaim, RunJobRefusal>> {
        match self
            .call::<_, ManualClaimReply>(
                callbacks::CLAIM_MANUAL_OCCURRENCE,
                &(job, owner, now, lease.num_milliseconds()),
            )
            .await?
        {
            ManualClaimReply::Claimed { claim } => Ok(Ok(*claim)),
            ManualClaimReply::Refused { reason } => Ok(Err(reason)),
        }
    }
    async fn renew_occurrence(
        &self,
        claim: &OccurrenceClaim,
        now: DateTime<Utc>,
        lease: chrono::Duration,
    ) -> Result<bool> {
        self.call(
            callbacks::RENEW_OCCURRENCE,
            &(&claim.receipt, now, lease.num_milliseconds()),
        )
        .await
    }
    async fn release_occurrence(
        &self,
        claim: &OccurrenceClaim,
        now: DateTime<Utc>,
    ) -> Result<bool> {
        self.call(callbacks::RELEASE_OCCURRENCE, &(&claim.receipt, now))
            .await
    }
    async fn queue_occurrence(&self, claim: &OccurrenceClaim, record: RunRecord) -> Result<bool> {
        self.call(callbacks::QUEUE_OCCURRENCE, &(&claim.receipt, record))
            .await
    }
    async fn complete_occurrence(
        &self,
        claim: &OccurrenceClaim,
        record: RunRecord,
        completion: Completion,
    ) -> Result<bool> {
        self.call(
            callbacks::COMPLETE_OCCURRENCE,
            &(&claim.receipt, record, completion),
        )
        .await
    }
}
#[async_trait]
impl JobExecutor for HostCallbacks {
    async fn execute(&self, job: &CronJob, run_id: &str) -> Result<Attempt> {
        self.call(callbacks::EXECUTE, &(job, run_id)).await
    }
    async fn failed(&self, job: &CronJob, run_id: &str, attempt: &Attempt) {
        if let Err(error) = self
            .call::<_, ()>(callbacks::FAILED, &(job, run_id, attempt))
            .await
        {
            tracing::warn!(%error, "scheduler failure notification failed");
        }
    }
}
#[async_trait]
impl RunDelivery for HostCallbacks {
    async fn deliver(
        &self,
        projection: &tinyflows_bus::schedule::DeliveryProjection,
    ) -> Result<()> {
        self.call(callbacks::DELIVER, &(projection,)).await
    }
    async fn send(&self, job: &CronJob, run_id: &str, attempt: &Attempt) -> Result<DeliveryStatus> {
        self.call(callbacks::SEND, &(job, run_id, attempt)).await
    }
}
#[async_trait]
impl TriggerSink for HostCallbacks {
    async fn schedule(&self, job: &CronJob, run_id: &str) -> Result<Attempt> {
        self.call(callbacks::SCHEDULE_TRIGGER, &(job, run_id)).await
    }
}
