use super::*;
struct QueueFailureStore {
    queue_calls: std::sync::atomic::AtomicUsize,
    releases: std::sync::atomic::AtomicUsize,
}
#[async_trait::async_trait]
impl tinyflows_schedule::runtime::JobStore for QueueFailureStore {
    async fn due_jobs(
        &self,
        _: chrono::DateTime<chrono::Utc>,
    ) -> anyhow::Result<Vec<CronJob>> {
        Ok(vec![])
    }
    async fn queue_occurrence(
        &self,
        _: &tinyflows_bus::schedule::OccurrenceClaim,
        _: tinyflows_schedule::runtime::RunRecord,
    ) -> anyhow::Result<bool> {
        if self.queue_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
            anyhow::bail!("injected queue failure")
        }
        Ok(true)
    }
    async fn release_occurrence(
        &self,
        _: &tinyflows_bus::schedule::OccurrenceClaim,
        _: chrono::DateTime<chrono::Utc>,
    ) -> anyhow::Result<bool> {
        self.releases
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(true)
    }
}

#[tokio::test]
async fn a_queue_error_releases_the_claim_and_cannot_be_reported_as_accepted() {
    let store = QueueFailureStore {
        queue_calls: std::sync::atomic::AtomicUsize::new(0),
        releases: std::sync::atomic::AtomicUsize::new(0),
    };
    let now = chrono::Utc::now();
    let job: CronJob = serde_json::from_value(json!({
        "id":"queued-before-ack","expression":"",
        "schedule":{"kind":"every","every_ms":60000},"command":"echo",
        "job_type":"shell","session_target":"isolated","enabled":true,
        "delivery":{"mode":"none"},"delete_after_run":false,
        "created_at":now,"next_run":now
    }))
    .unwrap();
    let claim = tinyflows_bus::schedule::OccurrenceClaim {
        receipt: "opaque-receipt".into(),
        job: job.clone(),
        occurrence: now,
        started_at: now,
        owner: "manual-owner".into(),
        run_id: Some("manual-run".into()),
    };
    let record = tinyflows_schedule::runtime::RunRecord {
        run_id: "manual-run".into(),
        job_id: job.id.clone(),
        job_created_at: job.created_at,
        started_at: now,
        finished_at: now,
        status: "queued".into(),
        output: String::new(),
        delivery_status: None,
    };
    assert!(
        super::persist_manual_queue(&store, &claim, record.clone(), now)
            .await
            .is_err()
    );
    assert_eq!(store.releases.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(super::persist_manual_queue(&store, &claim, record, now)
        .await
        .unwrap());
    assert_eq!(store.releases.load(std::sync::atomic::Ordering::SeqCst), 1);
}
