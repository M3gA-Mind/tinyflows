use super::*;

async fn parked_retry() -> (
    Arc<Fixture>,
    String,
    tokio::task::JoinHandle<Result<ManualRunOutcome>>,
) {
    let mut fixture = Fixture::new();
    let inner = Arc::get_mut(&mut fixture).unwrap();
    inner.fail_count = 1;
    inner.control_retry_heartbeat.store(true, Ordering::SeqCst);
    let job = fixture.job();
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
async fn lease_takeover_beats_a_ready_retry_and_heartbeat() {
    let (fixture, job_id, task) = parked_retry().await;
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
    let (fixture, job_id, task) = parked_retry().await;
    transfer_claim(&fixture, job_id);
    fixture.release_backoff.notify_one();

    assert!(task.await.unwrap().is_err());
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
    assert!(fixture.records.lock().unwrap().is_empty());
    assert!(fixture.projections.lock().unwrap().is_empty());
}
