use super::*;

async fn parked_retry(
    flow: bool,
) -> (
    Arc<Fixture>,
    String,
    tokio::task::JoinHandle<Result<ManualRunOutcome>>,
) {
    let mut fixture = Fixture::new();
    let inner = Arc::get_mut(&mut fixture).unwrap();
    if flow {
        inner.trigger_fail_count = 1;
    } else {
        inner.fail_count = 1;
    }
    inner.control_retry_heartbeat.store(true, Ordering::SeqCst);
    let mut job = fixture.job();
    if flow {
        job.job_type = crate::JobType::Flow;
    }
    let job_id = job.id.clone();
    let claim = fixture
        .claim_due_occurrence(
            &job,
            "original-owner",
            fixture.now(),
            chrono::Duration::seconds(10),
        )
        .await
        .unwrap()
        .unwrap();
    let clock = fixture.clone();
    let task = tokio::spawn(async move {
        let scheduler = Scheduler::new(job.id.clone(), SchedulerConfig::default(), clock.ports());
        scheduler
            .run_manual_claim(claim, ManualDelivery::Requested)
            .await
    });
    fixture.backoff_entered.notified().await;
    fixture.heartbeat_entered.notified().await;
    (fixture, job_id, task)
}

fn transfer_claim(fixture: &Fixture, job_id: String) {
    fixture.leases.lock().unwrap().insert(
        job_id,
        (
            "successor-owner".into(),
            fixture.now() + chrono::Duration::hours(2),
        ),
    );
}

#[tokio::test]
async fn last_nonempty_raw_detail_survives_a_later_detail_free_retry() {
    let mut fixture = Fixture::new();
    Arc::get_mut(&mut fixture).unwrap().fail_count = 1;
    fixture
        .raw_details
        .lock()
        .unwrap()
        .extend([Some("opaque host classification".into()), None]);
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
async fn lease_takeover_beats_a_ready_retry_and_heartbeat() {
    let (fixture, job_id, task) = parked_retry(false).await;
    transfer_claim(&fixture, job_id);
    fixture.release_backoff.notify_one();
    fixture.release_heartbeat.notify_one();

    assert!(task.await.unwrap().is_err());
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
    assert!(fixture.records.lock().unwrap().is_empty());
    assert!(fixture.projections.lock().unwrap().is_empty());
}

#[tokio::test]
async fn lease_takeover_blocks_retry_when_heartbeat_is_still_parked() {
    let (fixture, job_id, task) = parked_retry(false).await;
    transfer_claim(&fixture, job_id);
    fixture.release_backoff.notify_one();

    assert!(task.await.unwrap().is_err());
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
    assert!(fixture.records.lock().unwrap().is_empty());
    assert!(fixture.projections.lock().unwrap().is_empty());
}

#[tokio::test]
async fn lease_takeover_blocks_a_flow_trigger_retry() {
    let (fixture, job_id, task) = parked_retry(true).await;
    transfer_claim(&fixture, job_id);
    fixture.release_backoff.notify_one();

    assert!(task.await.unwrap().is_err());
    assert_eq!(fixture.triggers.load(Ordering::SeqCst), 1);
    assert!(fixture.records.lock().unwrap().is_empty());
    assert!(fixture.projections.lock().unwrap().is_empty());
}

struct HeldPermanentFailure {
    started: Notify,
    release: Notify,
    failed: AtomicUsize,
}

#[async_trait]
impl JobExecutor for HeldPermanentFailure {
    async fn execute(&self, _: &crate::CronJob, _: &str) -> Result<Attempt> {
        self.started.notify_one();
        self.release.notified().await;
        Ok(Attempt {
            success: false,
            output: "permanent failure".into(),
            retryable: false,
            raw_detail: None,
        })
    }

    async fn failed(&self, _: &crate::CronJob, _: &str, _: &Attempt) {
        self.failed.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn lease_takeover_fences_final_failure_callback_after_an_inflight_attempt() {
    let fixture = Fixture::new();
    let job = fixture.job();
    let claim = fixture
        .claim_due_occurrence(
            &job,
            "original-owner",
            fixture.now(),
            chrono::Duration::seconds(10),
        )
        .await
        .unwrap()
        .unwrap();
    let executor = Arc::new(HeldPermanentFailure {
        started: Notify::new(),
        release: Notify::new(),
        failed: AtomicUsize::new(0),
    });
    let mut ports = fixture.ports();
    ports.executor = executor.clone();
    let scheduler = Scheduler::new(job.id.clone(), SchedulerConfig::default(), ports);
    let task = tokio::spawn(async move {
        scheduler
            .run_manual_claim(claim, ManualDelivery::Requested)
            .await
    });
    executor.started.notified().await;
    transfer_claim(&fixture, job.id.clone());
    executor.release.notify_one();

    assert!(task.await.unwrap().is_err());
    assert_eq!(executor.failed.load(Ordering::SeqCst), 0);
    assert!(fixture.records.lock().unwrap().is_empty());
    assert!(fixture.projections.lock().unwrap().is_empty());
}
