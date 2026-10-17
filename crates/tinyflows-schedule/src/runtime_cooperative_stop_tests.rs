use super::*;
use crate::{CronJob, DeliveryStatus, JobType, Schedule};
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tinyflows_bus::schedule::{Attempt, Completion, DeliveryProjection, OccurrenceClaim, RunRecord};
use tokio::sync::Notify;

struct Fixture {
    now: DateTime<Utc>,
    block_on_renewal: usize,
    renewals: AtomicUsize,
    renewal_entered: Notify,
    release_renewal: Notify,
    executions: AtomicUsize,
    failed_callbacks: AtomicUsize,
    deliveries: AtomicUsize,
    fail_execution: bool,
}

impl Fixture {
    fn job(&self) -> CronJob {
        CronJob {
            claim_generation: 0,
            id: "cooperative-stop-job".into(),
            expression: String::new(),
            command: "echo stop".into(),
            prompt: None,
            name: None,
            schedule: Schedule::Every { every_ms: 60_000 },
            job_type: JobType::Shell,
            session_target: Default::default(),
            model: None,
            agent_id: None,
            enabled: true,
            delivery: Default::default(),
            delete_after_run: false,
            created_at: self.now,
            next_run: self.now,
            last_run: None,
            last_status: None,
            last_output: None,
            origin: None,
            policy: tinyflows_bus::schedule::JobPolicy {
                retries: Some(0),
                ..Default::default()
            },
        }
    }

    fn claim(&self) -> OccurrenceClaim {
        let job = self.job();
        OccurrenceClaim {
            receipt: "cooperative-stop-claim".into(),
            occurrence: self.now,
            started_at: self.now,
            owner: "cooperative-stop-owner".into(),
            job,
            run_id: Some("cooperative-stop-run".into()),
        }
    }

    fn ports(self: &Arc<Self>) -> RuntimePorts {
        RuntimePorts {
            store: self.clone(),
            executor: self.clone(),
            delivery: self.clone(),
            triggers: self.clone(),
            clock: self.clone(),
        }
    }
}

#[async_trait]
impl JobStore for Fixture {
    async fn due_jobs(&self, _: DateTime<Utc>) -> Result<Vec<CronJob>> {
        Ok(vec![])
    }

    async fn renew_occurrence(
        &self,
        _: &OccurrenceClaim,
        _: DateTime<Utc>,
        _: chrono::Duration,
    ) -> Result<bool> {
        let renewal = self.renewals.fetch_add(1, Ordering::SeqCst) + 1;
        if renewal == self.block_on_renewal {
            self.renewal_entered.notify_one();
            self.release_renewal.notified().await;
        }
        Ok(true)
    }

    async fn release_occurrence(&self, _: &OccurrenceClaim, _: DateTime<Utc>) -> Result<bool> {
        Ok(true)
    }

    async fn complete_occurrence(
        &self,
        _: &OccurrenceClaim,
        _: RunRecord,
        _: Completion,
    ) -> Result<bool> {
        Ok(true)
    }
}

#[async_trait]
impl JobExecutor for Fixture {
    async fn execute(&self, _: &CronJob, _: &str) -> Result<Attempt> {
        self.executions.fetch_add(1, Ordering::SeqCst);
        Ok(Attempt {
            success: !self.fail_execution,
            output: "result".into(),
            retryable: false,
            raw_detail: None,
        })
    }

    async fn failed(&self, _: &CronJob, _: &str, _: &Attempt) {
        self.failed_callbacks.fetch_add(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl RunDelivery for Fixture {
    async fn send(&self, _: &CronJob, _: &str, _: &Attempt) -> Result<DeliveryStatus> {
        self.deliveries.fetch_add(1, Ordering::SeqCst);
        Ok(DeliveryStatus::NotRequested)
    }

    async fn deliver(&self, _: &DeliveryProjection) -> Result<()> {
        self.deliveries.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl TriggerSink for Fixture {
    async fn schedule(&self, _: &CronJob, _: &str) -> Result<Attempt> {
        self.executions.fetch_add(1, Ordering::SeqCst);
        Ok(Attempt {
            success: !self.fail_execution,
            output: "triggered".into(),
            retryable: false,
            raw_detail: None,
        })
    }
}

#[async_trait]
impl Clock for Fixture {
    fn now(&self) -> DateTime<Utc> {
        self.now
    }

    async fn sleep(&self, _: Duration) {
        std::future::pending().await
    }

    async fn lease_wait(&self, _: Duration) {
        std::future::pending().await
    }
}

fn fixture(block_on_renewal: usize, fail_execution: bool) -> Arc<Fixture> {
    Arc::new(Fixture {
        now: "2026-01-01T00:00:00Z".parse().unwrap(),
        block_on_renewal,
        renewals: AtomicUsize::new(0),
        renewal_entered: Notify::new(),
        release_renewal: Notify::new(),
        executions: AtomicUsize::new(0),
        failed_callbacks: AtomicUsize::new(0),
        deliveries: AtomicUsize::new(0),
        fail_execution,
    })
}

async fn stop_during_renewal(block_on_renewal: usize, fail_execution: bool) -> Arc<Fixture> {
    let fixture = fixture(block_on_renewal, fail_execution);
    let scheduler = Scheduler::new("cooperative-stop", SchedulerConfig::default(), fixture.ports());
    let claim = fixture.claim();
    let (stop, receiver) = watch::channel(false);
    let worker = tokio::spawn(async move {
        scheduler
            .run_manual_claim_cooperative(claim, ManualDelivery::NotRequested, receiver)
            .await
    });
    fixture.renewal_entered.notified().await;
    stop.send_replace(true);
    fixture.release_renewal.notify_one();
    assert!(worker.await.unwrap().unwrap().is_none());
    fixture
}

#[tokio::test]
async fn stop_during_initial_claim_renewal_prevents_execution_callback() {
    let fixture = stop_during_renewal(1, false).await;
    assert_eq!(fixture.executions.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.failed_callbacks.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.deliveries.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn stop_during_failure_renewal_prevents_failure_callback() {
    let fixture = stop_during_renewal(2, true).await;
    assert_eq!(fixture.executions.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.failed_callbacks.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.deliveries.load(Ordering::SeqCst), 0);
}
