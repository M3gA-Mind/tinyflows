use super::{CronDocuments, test_support::docs_in};
use chrono::{Duration, Utc};
use tinystoragedrivers_core::MemoryStorage;
use tinyflows_schedule::{DeliveryStatus, Schedule};

#[tokio::test]
async fn forced_manual_claim_queues_before_ack_and_replaces_queue_atomically() {
    let backend = MemoryStorage::new();
    let store = CronDocuments::new(docs_in(&backend, "manual-claim"));
    let mut job = store
        .add_shell_job(
            None,
            Schedule::At {
                at: Utc::now() + Duration::hours(1),
            },
            "echo manual",
        )
        .await
        .unwrap();
    job.enabled = false;
    store.disable_job(&job.id).await.unwrap();
    let now = Utc::now();

    assert!(
        store
            .claim_due_job(&job.id, "scheduled", now, Duration::minutes(1))
            .await
            .unwrap()
            .is_none(),
        "scheduled admission must continue to reject disabled future jobs"
    );
    let claim = store
        .claim_job_now(&job.id, "manual-owner", now, Duration::minutes(1))
        .await
        .unwrap()
        .expect("manual run may force the saved disabled future job");
    assert_eq!(claim.job().id, job.id);
    assert!(!store
        .claim_job_now(&job.id, "second-manual", now, Duration::minutes(1))
        .await
        .unwrap()
        .is_some());
    assert!(store.queue_claimed_run(&claim, now).await.unwrap());
    let queued = store.list_runs(&job.id, 10).await.unwrap();
    assert_eq!(queued.len(), 1);
    assert_eq!(queued[0].status, "queued");

    assert!(store
        .complete_claim(
            &claim,
            now + Duration::seconds(1),
            true,
            "ran once",
            Some(DeliveryStatus::NotRequested),
        )
        .await
        .unwrap());
    let history = store.list_runs(&job.id, 10).await.unwrap();
    assert_eq!(history.len(), 1, "the queue placeholder is replaced at terminal commit");
    assert_eq!(history[0].status, "success");
    assert_eq!(history[0].output.as_deref(), Some("ran once"));
    assert_eq!(store.get_job(&job.id).await.unwrap().enabled, false);
}
