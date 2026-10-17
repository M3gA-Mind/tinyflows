use super::*;
use crate::{DeliveryStatus, JobType, Schedule};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tinyflows_bus::schedule::{OccurrenceClaim, RunJobRefusal};
use tinyflows_bus::schedule::DeliveryProjection;

struct Fixture {
    now: Mutex<DateTime<Utc>>,
    waits: Mutex<Vec<Duration>>,
    jobs: Mutex<Vec<CronJob>>,
    leases: Mutex<HashMap<String, (String, DateTime<Utc>)>>,
    records: Mutex<Vec<RunRecord>>,
    completions: Mutex<Vec<Completion>>,
    projections: Mutex<Vec<DeliveryProjection>>,
    raw_details: Mutex<Vec<Option<String>>>,
    calls: AtomicUsize,
    triggers: AtomicUsize,
    fail_count: usize,
    permanent: bool,
    delivery_fails: bool,
    delivery_status_failed: bool,
    claim_fails: bool,
    max_tasks: usize,
}
impl Fixture {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            now: Mutex::new("2026-01-01T00:00:00Z".parse().unwrap()),
            waits: Mutex::new(vec![]),
            jobs: Mutex::new(vec![]),
            leases: Mutex::new(HashMap::new()),
            records: Mutex::new(vec![]),
            completions: Mutex::new(vec![]),
            projections: Mutex::new(vec![]),
            raw_details: Mutex::new(vec![]),
            calls: AtomicUsize::new(0),
            triggers: AtomicUsize::new(0),
            fail_count: 0,
            permanent: false,
            delivery_fails: false,
            delivery_status_failed: false,
            claim_fails: false,
            max_tasks: usize::MAX,
        })
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
    fn job(&self) -> CronJob {
        let now = self.now();
        CronJob {
            claim_generation: 0,
            id: uuid::Uuid::new_v4().to_string(),
            expression: String::new(),
            command: "echo ok".into(),
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
            created_at: now,
            next_run: now,
            last_run: None,
            last_status: None,
            last_output: None,
            origin: None,
            policy: Default::default(),
        }
    }
}
#[async_trait]
impl Clock for Fixture {
    fn now(&self) -> DateTime<Utc> {
        *self.now.lock().unwrap()
    }
    async fn sleep(&self, duration: Duration) {
        self.waits.lock().unwrap().push(duration);
        *self.now.lock().unwrap() += chrono::Duration::from_std(duration).unwrap();
    }
    async fn lease_wait(&self, _: Duration) {
        std::future::pending().await
    }
}
#[async_trait]
impl JobStore for Fixture {
    async fn due_jobs(&self, _: DateTime<Utc>) -> Result<Vec<CronJob>> {
        Ok(self
            .jobs
            .lock()
            .unwrap()
            .iter()
            .take(self.max_tasks)
            .cloned()
            .collect())
    }
    async fn due_jobs_excluding(
        &self,
        _: DateTime<Utc>,
        excluded: &[String],
    ) -> Result<Vec<CronJob>> {
        Ok(self
            .jobs
            .lock()
            .unwrap()
            .iter()
            .filter(|job| !excluded.contains(&job.id))
            .take(self.max_tasks)
            .cloned()
            .collect())
    }
    async fn claim_due_occurrence(
        &self,
        job: &CronJob,
        owner: &str,
        now: DateTime<Utc>,
        lease: chrono::Duration,
    ) -> Result<Option<OccurrenceClaim>> {
        anyhow::ensure!(!self.claim_fails, "durable claim rejected");
        let mut leases = self.leases.lock().unwrap();
        if leases.get(&job.id).is_some_and(|(_, expires)| *expires > now) {
            return Ok(None);
        }
        let token = uuid::Uuid::new_v4().to_string();
        leases.insert(job.id.clone(), (token.clone(), now + lease));
        Ok(Some(OccurrenceClaim {
            receipt: token,
            job: job.clone(),
            occurrence: job.next_run,
            started_at: now,
            owner: owner.into(),
            run_id: None,
        }))
    }
    async fn claim_manual_occurrence(
        &self,
        job: &CronJob,
        owner: &str,
        now: DateTime<Utc>,
        lease: chrono::Duration,
    ) -> Result<Result<OccurrenceClaim, RunJobRefusal>> {
        match self
            .claim_due_occurrence(job, owner, now, lease)
            .await?
        {
            Some(claim) => Ok(Ok(claim)),
            None => Ok(Err(RunJobRefusal::AlreadyRunning)),
        }
    }
    async fn renew_occurrence(
        &self,
        claim: &OccurrenceClaim,
        now: DateTime<Utc>,
        lease: chrono::Duration,
    ) -> Result<bool> {
        let mut leases = self.leases.lock().unwrap();
        let Some((token, expires)) = leases.get_mut(&claim.job.id) else {
            return Ok(false);
        };
        if token != &claim.receipt || *expires <= now {
            return Ok(false);
        }
        *expires = (*expires).max(now + lease);
        Ok(true)
    }
    async fn release_occurrence(
        &self,
        claim: &OccurrenceClaim,
        _: DateTime<Utc>,
    ) -> Result<bool> {
        let mut leases = self.leases.lock().unwrap();
        if leases
            .get(&claim.job.id)
            .is_some_and(|(token, _)| token == &claim.receipt)
        {
            leases.remove(&claim.job.id);
            return Ok(true);
        }
        Ok(false)
    }
    async fn queue_occurrence(
        &self,
        _: &OccurrenceClaim,
        record: RunRecord,
    ) -> Result<bool> {
        self.records.lock().unwrap().push(record);
        Ok(true)
    }
    async fn complete_occurrence(
        &self,
        claim: &OccurrenceClaim,
        record: RunRecord,
        completion: Completion,
    ) -> Result<bool> {
        let mut leases = self.leases.lock().unwrap();
        if !leases
            .get(&claim.job.id)
            .is_some_and(|(token, expires)| token == &claim.receipt && *expires > record.finished_at)
        {
            return Ok(false);
        }
        leases.remove(&claim.job.id);
        self.records.lock().unwrap().push(record);
        self.completions.lock().unwrap().push(completion);
        Ok(true)
    }
}
#[async_trait]
impl JobExecutor for Fixture {
    async fn execute(&self, _: &CronJob, _: &str) -> Result<Attempt> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        let raw_detail = {
            let mut details = self.raw_details.lock().unwrap();
            if details.is_empty() {
                None
            } else {
                details.remove(0)
            }
        };
        Ok(Attempt {
            success: call >= self.fail_count,
            output: "result".into(),
            retryable: !self.permanent,
            raw_detail,
        })
    }
}
#[async_trait]
impl RunDelivery for Fixture {
    async fn send(&self, _: &CronJob, _: &str, _: &Attempt) -> Result<DeliveryStatus> {
        if self.delivery_fails {
            anyhow::bail!("delivery unavailable");
        }
        Ok(if self.delivery_status_failed {
            DeliveryStatus::Failed
        } else {
            DeliveryStatus::NotRequested
        })
    }
    async fn deliver(&self, projection: &DeliveryProjection) -> Result<()> {
        self.projections.lock().unwrap().push(projection.clone());
        Ok(())
    }
}
#[async_trait]
impl TriggerSink for Fixture {
    async fn schedule(&self, _: &CronJob, _: &str) -> Result<Attempt> {
        self.triggers.fetch_add(1, Ordering::SeqCst);
        Ok(Attempt {
            success: true,
            output: "triggered".into(),
            retryable: false,
            raw_detail: None,
        })
    }
}

#[tokio::test]
async fn retries_use_clock_and_zero_override_is_one_attempt() {
    let mut f = Fixture::new();
    Arc::get_mut(&mut f).unwrap().fail_count = 2;
    let job = f.job();
    let result = execute_with_retry(&f.ports(), &SchedulerConfig::default(), &job, "stable")
        .await
        .unwrap();
    assert!(result.success);
    assert_eq!(f.calls.load(Ordering::SeqCst), 3);
    assert_eq!(
        *f.waits.lock().unwrap(),
        [Duration::from_millis(200), Duration::from_millis(600)]
    );
    let mut job = f.job();
    job.policy.retries = Some(0);
    f.calls.store(0, Ordering::SeqCst);
    assert!(
        !execute_with_retry(&f.ports(), &SchedulerConfig::default(), &job, "stable")
            .await
            .unwrap()
            .success
    );
    assert_eq!(f.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn last_nonempty_raw_detail_survives_a_later_detail_free_retry() {
    let mut fixture = Fixture::new();
    Arc::get_mut(&mut fixture).unwrap().fail_count = 1;
    fixture.raw_details.lock().unwrap().extend([
        Some("opaque host classification".into()),
        None,
    ]);
    let mut job = fixture.job();
    job.policy.retries = Some(1);
    let result = execute_with_retry(
        &fixture.ports(),
        &SchedulerConfig::default(),
        &job,
        "raw-detail-run",
    )
    .await
    .unwrap();
    assert!(result.success);
    assert_eq!(
        result.raw_detail.as_deref(),
        Some("opaque host classification")
    );
}

#[tokio::test]
async fn permanent_failure_halts_without_waiting() {
    let mut f = Fixture::new();
    let inner = Arc::get_mut(&mut f).unwrap();
    inner.fail_count = 10;
    inner.permanent = true;
    assert!(
        !execute_with_retry(&f.ports(), &SchedulerConfig::default(), &f.job(), "run")
            .await
            .unwrap()
            .success
    );
    assert_eq!(f.calls.load(Ordering::SeqCst), 1);
    assert!(f.waits.lock().unwrap().is_empty());
}

#[tokio::test]
async fn one_shots_disable_unless_successful_auto_delete() {
    for delete in [false, true] {
        let f = Fixture::new();
        let mut job = f.job();
        job.schedule = Schedule::At { at: f.now() };
        job.delete_after_run = delete;
        let mut scheduler = Scheduler::new(job.id.clone(), SchedulerConfig::default(), f.ports());
        scheduler.dispatch(vec![job]).await.unwrap();
        scheduler.drain().await.unwrap();
        let c = f.completions.lock().unwrap();
        assert_eq!(c[0].remove, delete);
        assert_eq!(c[0].disable, !delete);
        assert!(c[0].next_run.is_none());
    }
}

#[tokio::test]
async fn flow_schedule_dispatches_directly_to_trigger_sink() {
    let f = Fixture::new();
    let mut job = f.job();
    job.job_type = JobType::Flow;
    let mut scheduler = Scheduler::new(job.id.clone(), SchedulerConfig::default(), f.ports());
    scheduler.dispatch(vec![job]).await.unwrap();
    scheduler.drain().await.unwrap();
    assert_eq!(f.triggers.load(Ordering::SeqCst), 1);
    assert_eq!(f.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn required_delivery_failure_disables_failed_one_shot_without_deleting() {
    let mut f = Fixture::new();
    Arc::get_mut(&mut f).unwrap().delivery_fails = true;
    let mut job = f.job();
    job.schedule = Schedule::At { at: f.now() };
    job.delete_after_run = true;
    job.delivery.best_effort = false;
    let mut scheduler = Scheduler::new(job.id.clone(), SchedulerConfig::default(), f.ports());
    scheduler.dispatch(vec![job]).await.unwrap();
    scheduler.drain().await.unwrap();
    let c = f.completions.lock().unwrap();
    assert!(c[0].disable);
    assert!(!c[0].remove);
    assert!(!c[0].success);
    let r = f.records.lock().unwrap();
    assert_eq!(r[0].status, "error");
    assert_eq!(r[0].delivery_status, Some(DeliveryStatus::Failed));
    let projection = &f.projections.lock().unwrap()[0];
    assert!(!projection.success, "terminal delivery sees required-send failure");
    assert_eq!(projection.delivery_status, Some(DeliveryStatus::Failed));
}

#[tokio::test]
async fn durable_claim_blocks_a_second_dispatch_without_advancing_the_occurrence() {
    let f = Fixture::new();
    let mut job = f.job();
    job.policy.single_flight = true;
    let scope = job.id.clone();
    let held = f
        .claim_due_occurrence(&job, "first-owner", f.now(), chrono::Duration::hours(1))
        .await
        .unwrap()
        .unwrap();
    let mut scheduler = Scheduler::new(scope, SchedulerConfig::default(), f.ports());
    assert_eq!(scheduler.dispatch(vec![job]).await.unwrap(), 0);
    assert!(f.records.lock().unwrap().is_empty());
    assert_eq!(f.calls.load(Ordering::SeqCst), 0);
    assert_eq!(held.occurrence, f.job().next_run);
}

#[test]
fn scheduler_slots_are_scoped_and_release_on_drop() {
    let a = uuid::Uuid::new_v4().to_string();
    let slot = SchedulerSlot::acquire(&a).unwrap();
    assert!(SchedulerSlot::acquire(&a).is_none());
    drop(slot);
    assert!(SchedulerSlot::acquire(&a).is_some());
}

struct BlockExecutor {
    started: Semaphore,
    release: Semaphore,
    active: AtomicUsize,
    peak: AtomicUsize,
}
#[async_trait]
impl JobExecutor for BlockExecutor {
    async fn execute(&self, _: &CronJob, _: &str) -> Result<Attempt> {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        self.started.add_permits(1);
        self.release.acquire().await.unwrap().forget();
        self.active.fetch_sub(1, Ordering::SeqCst);
        Ok(Attempt {
            success: true,
            output: "ok".into(),
            retryable: false,
            raw_detail: None,
        })
    }
}

#[tokio::test]
async fn dispatch_returns_before_execution_and_limits_concurrent_jobs() {
    let f = Fixture::new();
    let executor = Arc::new(BlockExecutor {
        started: Semaphore::new(0),
        release: Semaphore::new(0),
        active: AtomicUsize::new(0),
        peak: AtomicUsize::new(0),
    });
    let mut ports = f.ports();
    ports.executor = executor.clone();
    let mut scheduler = Scheduler::new(
        uuid::Uuid::new_v4().to_string(),
        SchedulerConfig {
            max_concurrent: 1,
            ..Default::default()
        },
        ports,
    );
    assert_eq!(scheduler.dispatch(vec![f.job(), f.job()]).await.unwrap(), 2);
    executor.started.acquire().await.unwrap().forget();
    assert_eq!(executor.active.load(Ordering::SeqCst), 1);
    executor.release.add_permits(1);
    executor.started.acquire().await.unwrap().forget();
    executor.release.add_permits(1);
    scheduler.drain().await.unwrap();
    assert_eq!(executor.peak.load(Ordering::SeqCst), 1);
    assert_eq!(f.records.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn shutdown_aborts_queued_and_running_jobs_and_releases_claims() {
    let f = Fixture::new();
    let executor = Arc::new(BlockExecutor {
        started: Semaphore::new(0),
        release: Semaphore::new(0),
        active: AtomicUsize::new(0),
        peak: AtomicUsize::new(0),
    });
    let mut ports = f.ports();
    ports.executor = executor.clone();
    let scope = uuid::Uuid::new_v4().to_string();
    let jobs = vec![f.job(), f.job()];
    let ids: Vec<_> = jobs.iter().map(|j| j.id.clone()).collect();
    let mut scheduler = Scheduler::new(
        scope.clone(),
        SchedulerConfig {
            max_concurrent: 1,
            ..Default::default()
        },
        ports,
    );
    scheduler.dispatch(jobs).await.unwrap();
    executor.started.acquire().await.unwrap().forget();
    assert!(ids.iter().all(|id| f.leases.lock().unwrap().contains_key(id)));
    scheduler.shutdown().await;
    assert!(ids.iter().all(|id| !f.leases.lock().unwrap().contains_key(id)));
    assert!(scheduler.dispatch(vec![f.job()]).await.is_err());
}

#[tokio::test]
async fn rejected_durable_claim_never_dispatches_and_releases_local_claim() {
    let mut f = Fixture::new();
    Arc::get_mut(&mut f).unwrap().claim_fails = true;
    let job = f.job();
    let scope = job.id.clone();
    let mut scheduler = Scheduler::new(scope.clone(), SchedulerConfig::default(), f.ports());
    assert!(scheduler.dispatch(vec![job.clone()]).await.is_err());
    scheduler.drain().await.unwrap();
    assert_eq!(f.calls.load(Ordering::SeqCst), 0);
    assert!(!f.leases.lock().unwrap().contains_key(&job.id));
}

#[tokio::test]
async fn one_shot_execution_requires_a_durable_claim() {
    let mut f = Fixture::new();
    Arc::get_mut(&mut f).unwrap().claim_fails = true;
    let mut job = f.job();
    job.schedule = Schedule::At { at: f.now() };
    let mut scheduler = Scheduler::new(job.id.clone(), SchedulerConfig::default(), f.ports());
    assert!(scheduler.dispatch(vec![job]).await.is_err());
    scheduler.drain().await.unwrap();
    assert_eq!(f.calls.load(Ordering::SeqCst), 0);
}

#[test]
fn execution_definition_includes_every_execution_and_delivery_input() {
    let f = Fixture::new();
    let job = f.job();
    let original = serde_json::to_value(&job).unwrap();
    for (field, value) in [
        ("command", serde_json::json!("changed")),
        ("prompt", serde_json::json!("changed")),
        ("job_type", serde_json::json!("agent")),
        ("session_target", serde_json::json!("main")),
        ("model", serde_json::json!("changed")),
        ("agent_id", serde_json::json!("changed")),
        (
            "delivery",
            serde_json::json!({"mode":"announce","best_effort":false}),
        ),
        (
            "origin",
            serde_json::json!({"kind":"web","thread_id":"changed"}),
        ),
        (
            "policy",
            serde_json::json!({"retries":0,"single_flight":true}),
        ),
        ("delete_after_run", serde_json::json!(true)),
    ] {
        let mut edited = original.clone();
        edited[field] = value;
        let edited: CronJob = serde_json::from_value(edited).unwrap();
        assert!(!job.same_execution_definition(&edited), "{field}");
    }
    let mut bookkeeping = job.clone();
    bookkeeping.name = Some("renamed".into());
    bookkeeping.last_output = Some("completed".into());
    assert!(job.same_execution_definition(&bookkeeping));
}

#[tokio::test]
async fn explicit_failed_delivery_prevents_auto_delete_only_when_required() {
    for best_effort in [false, true] {
        let mut f = Fixture::new();
        Arc::get_mut(&mut f).unwrap().delivery_status_failed = true;
        let mut job = f.job();
        job.schedule = Schedule::At { at: f.now() };
        job.delete_after_run = true;
        job.delivery.best_effort = best_effort;
        let mut scheduler = Scheduler::new(job.id.clone(), SchedulerConfig::default(), f.ports());
        scheduler.dispatch(vec![job]).await.unwrap();
        scheduler.drain().await.unwrap();
        let completions = f.completions.lock().unwrap();
        assert_eq!(completions[0].success, best_effort);
        assert_eq!(completions[0].remove, best_effort);
        assert_eq!(completions[0].disable, !best_effort);
        let records = f.records.lock().unwrap();
        assert_eq!(records[0].status, if best_effort { "ok" } else { "error" });
        assert_eq!(records[0].delivery_status, Some(DeliveryStatus::Failed));
        let projection = &f.projections.lock().unwrap()[0];
        assert_eq!(projection.success, best_effort);
        assert_eq!(projection.delivery_status, Some(DeliveryStatus::Failed));
    }
}

#[tokio::test]
async fn deferred_active_head_does_not_starve_later_due_jobs_in_a_limited_batch() {
    let mut fixture = Fixture::new();
    Arc::get_mut(&mut fixture).unwrap().max_tasks = 1;
    let first = fixture.job();
    let second = fixture.job();
    *fixture.jobs.lock().unwrap() = vec![first.clone(), second.clone()];
    let scope = uuid::Uuid::new_v4().to_string();
    let held = fixture
        .claim_due_occurrence(&first, "held", fixture.now(), chrono::Duration::hours(1))
        .await
        .unwrap()
        .unwrap();
    let mut scheduler = Scheduler::new(scope, SchedulerConfig::default(), fixture.ports());
    assert_eq!(scheduler.tick().await.unwrap(), 1);
    scheduler.drain().await.unwrap();
    assert_eq!(fixture.records.lock().unwrap()[0].job_id, second.id);
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.jobs.lock().unwrap()[0].next_run, first.next_run);
    assert_eq!(held.job.id, first.id);
}
