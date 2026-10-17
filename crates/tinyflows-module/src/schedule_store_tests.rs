use super::*;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::time::Duration;
use tokio::sync::Notify;
use tinyflows_schedule::runtime::*;
use tinyflows_schedule::{DeliveryStatus, Schedule};
use tinystoragedrivers_core::{DocumentStoreExt, MemoryStorage, Scope, StorageBackend};

struct Replacement(CronDocuments);
#[async_trait]
impl JobExecutor for Replacement {
    async fn execute(&self, job: &CronJob, _: &str) -> Result<Attempt> {
        self.0.remove_job(&job.id).await?;
        let replacement = self
            .0
            .add_flow_schedule_job("history-fence", job.schedule.clone())
            .await?;
        assert_eq!(job.id, replacement.id);
        assert_ne!(job.created_at, replacement.created_at);
        Ok(Attempt {
            success: true,
            output: "old incarnation".into(),
            retryable: false,
            raw_detail: None,
        })
    }
}
#[async_trait]
impl RunDelivery for Replacement {
    async fn send(&self, _: &CronJob, _: &str, _: &Attempt) -> Result<DeliveryStatus> {
        Ok(DeliveryStatus::NotRequested)
    }
    async fn deliver(&self, _: &tinyflows_bus::schedule::DeliveryProjection) -> Result<()> {
        Ok(())
    }
}
#[async_trait]
impl TriggerSink for Replacement {
    async fn schedule(&self, job: &CronJob, run_id: &str) -> Result<Attempt> {
        self.execute(job, run_id).await
    }
}
#[tokio::test]
async fn old_run_history_does_not_attach_to_a_recreated_flow_job() {
    let backend = MemoryStorage::new();
    let scoped = backend
        .for_scope(&Scope::new("history-fence").unwrap())
        .unwrap();
    let documents = CronDocuments::new(scoped.documents().clone());
    let job = documents
        .add_flow_schedule_job("history-fence", Schedule::Every { every_ms: 60_000 })
        .await
        .unwrap();
    let io = Arc::new(Replacement(documents.clone()));
    let ports = RuntimePorts {
        store: Arc::new(Store(documents.clone())),
        executor: io.clone(),
        delivery: io.clone(),
        triggers: io,
        clock: Arc::new(SystemClock),
    };
    let mut scheduler = Scheduler::new("history-fence", SchedulerConfig::default(), ports);
    scheduler.dispatch(vec![job.clone()]).await.unwrap();
    let error = scheduler.drain().await.unwrap_err();
    assert!(
        error.to_string().contains("removed job creation"),
        "{error:#}"
    );
    assert!(
        documents.list_runs(&job.id, 10).await.unwrap().is_empty(),
        "old execution contaminated the replacement job history"
    );
}

struct HeldRun {
    started: Notify,
}
#[async_trait]
impl JobExecutor for HeldRun {
    async fn execute(&self, _: &CronJob, _: &str) -> Result<Attempt> {
        self.started.notify_one();
        std::future::pending().await
    }
}

struct FrozenClock(std::sync::Mutex<DateTime<Utc>>);
#[async_trait]
impl Clock for FrozenClock {
    fn now(&self) -> DateTime<Utc> {
        *self.0.lock().unwrap()
    }
    async fn sleep(&self, _: Duration) {}
}

struct NoDelivery;
#[async_trait]
impl RunDelivery for NoDelivery {
    async fn send(&self, _: &CronJob, _: &str, _: &Attempt) -> Result<DeliveryStatus> {
        Ok(DeliveryStatus::NotRequested)
    }
    async fn deliver(&self, _: &tinyflows_bus::schedule::DeliveryProjection) -> Result<()> {
        Ok(())
    }
}
#[async_trait]
impl TriggerSink for NoDelivery {
    async fn schedule(&self, _: &CronJob, _: &str) -> Result<Attempt> {
        anyhow::bail!("not used")
    }
}

async fn assert_interrupted_occurrence_recovers(schedule: Schedule) {
    let now = Utc::now() - chrono::Duration::seconds(60);
    let backend = MemoryStorage::new();
    let scoped = backend.for_scope(&Scope::new("lease-recovery").unwrap()).unwrap();
    let documents = CronDocuments::new(scoped.documents().clone());
    let mut job = documents
        .add_flow_schedule_job("lease-recovery", schedule)
        .await
        .unwrap();
    job.next_run = now;
    let docs = scoped.documents().clone();
    let stored = docs.get("cron_jobs", &job.id).await.unwrap().unwrap();
    let mut body = stored.doc.clone();
    body["next_run"] = serde_json::json!(now.to_rfc3339());
    body["next_run_ms"] = serde_json::json!(now.timestamp_millis());
    body["next_run_ns"] = serde_json::json!(now.timestamp_nanos_opt().unwrap());
    docs.put("cron_jobs", &job.id, body, stored.unchanged())
        .await
        .unwrap();
    let frozen = Arc::new(FrozenClock(std::sync::Mutex::new(now)));
    let held = Arc::new(HeldRun { started: Notify::new() });
    let ports = RuntimePorts {
        store: Arc::new(Store(documents.clone())),
        executor: held.clone(),
        delivery: Arc::new(NoDelivery),
        triggers: Arc::new(NoDelivery),
        clock: frozen.clone(),
    };
    let mut first = Scheduler::new("lease-recovery", SchedulerConfig::default(), ports.clone());
    assert_eq!(first.dispatch(vec![job.clone()]).await.unwrap(), 1);
    held.started.notified().await;
    first.shutdown().await;
    *frozen.0.lock().unwrap() = now + chrono::Duration::seconds(30);

    let mut restarted = Scheduler::new("lease-recovery", SchedulerConfig::default(), ports);
    let recovered = restarted.tick().await.unwrap();
    assert_eq!(recovered, 1, "the interrupted occurrence must still be due");
    restarted.shutdown().await;
}

#[tokio::test]
async fn interrupted_recurring_occurrence_is_recovered_before_next_recurrence() {
    assert_interrupted_occurrence_recovers(Schedule::Every { every_ms: 60_000 }).await;
}

#[tokio::test]
async fn interrupted_one_shot_occurrence_is_recovered_after_restart() {
    assert_interrupted_occurrence_recovers(Schedule::At {
        at: "2026-01-01T00:00:00Z".parse().unwrap(),
    })
    .await;
}
