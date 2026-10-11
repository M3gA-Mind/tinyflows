//! Occurrences are leased across handles and fenced against stale workers.
use super::{
    CronDocuments,
    test_support::{daily, docs_in},
};
use chrono::{Duration, Utc};
use tinystoragedrivers_core::MemoryStorage;

#[tokio::test]
async fn two_handles_cannot_claim_the_same_occurrence() {
    let backend = MemoryStorage::new();
    let a = CronDocuments::new(docs_in(&backend, "claims"));
    let b = CronDocuments::new(docs_in(&backend, "claims"));
    let job = a.add_shell_job(None, daily(), "echo").await.unwrap();
    let now = job.next_run;
    let (first, second) = tokio::join!(
        a.claim_due_job(&job.id, "a", now, Duration::seconds(30)),
        b.claim_due_job(&job.id, "b", now, Duration::seconds(30))
    );
    assert_eq!(
        usize::from(first.unwrap().is_some()) + usize::from(second.unwrap().is_some()),
        1
    );
}

#[tokio::test]
async fn expired_claim_is_recovered_and_old_worker_is_fenced() {
    let a = super::test_support::store();
    let job = a.add_shell_job(None, daily(), "echo").await.unwrap();
    let now = job.next_run;
    let old = a
        .claim_due_job(&job.id, "old", now, Duration::seconds(30))
        .await
        .unwrap()
        .unwrap();
    let later = now + Duration::seconds(30);
    let fresh = a
        .claim_due_job(&job.id, "fresh", later, Duration::seconds(30))
        .await
        .unwrap()
        .unwrap();
    assert!(
        !a.renew_claim(&old, later, Duration::seconds(60))
            .await
            .unwrap()
    );
    assert!(!a.release_claim(&old, later).await.unwrap());
    assert!(
        !a.complete_claim(&old, later, true, "old result", None)
            .await
            .unwrap()
    );
    assert!(
        a.complete_claim(&fresh, later, true, "fresh result", None)
            .await
            .unwrap()
    );
    assert_eq!(
        a.get_job(&job.id).await.unwrap().last_output.as_deref(),
        Some("fresh result")
    );
    assert_eq!(a.list_runs(&job.id, 10).await.unwrap().len(), 1);
}
