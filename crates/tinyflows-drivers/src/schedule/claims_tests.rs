//! Occurrences are leased across handles and fenced against stale workers.
use super::{
    CronDocuments,
    test_support::{daily, docs_in},
};
use chrono::Duration;
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

#[tokio::test]
async fn renewal_never_shortens_an_existing_lease() {
    let a = super::test_support::store();
    let job = a.add_shell_job(None, daily(), "echo").await.unwrap();
    let now = job.next_run;
    let claim = a
        .claim_due_job(&job.id, "worker", now, Duration::seconds(60))
        .await
        .unwrap()
        .unwrap();
    assert!(
        a.renew_claim(&claim, now, Duration::seconds(1))
            .await
            .unwrap()
    );
    assert!(
        a.complete_claim(&claim, now + Duration::seconds(30), true, "ok", None)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn edited_canceled_and_recreated_jobs_reject_the_old_completion() {
    use tinyflows_schedule::CronJobPatch;
    let a = super::test_support::store();
    let job = a.add_flow_schedule_job("flow", daily()).await.unwrap();
    let now = job.next_run;
    let old = a
        .claim_due_job(&job.id, "worker", now, Duration::minutes(1))
        .await
        .unwrap()
        .unwrap();
    a.update_job(
        &job.id,
        CronJobPatch {
            enabled: Some(false),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(
        !a.complete_claim(&old, now, true, "canceled", None)
            .await
            .unwrap()
    );
    a.update_job(
        &job.id,
        CronJobPatch {
            enabled: Some(true),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let current = a.get_job(&job.id).await.unwrap();
    let old = a
        .claim_due_job(&job.id, "worker", current.next_run, Duration::minutes(1))
        .await
        .unwrap()
        .unwrap();
    a.update_job(
        &job.id,
        CronJobPatch {
            schedule: Some(daily()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(
        !a.complete_claim(&old, current.next_run, true, "edited", None)
            .await
            .unwrap()
    );
    let current = a.get_job(&job.id).await.unwrap();
    let old = a
        .claim_due_job(&job.id, "worker", current.next_run, Duration::minutes(1))
        .await
        .unwrap()
        .unwrap();
    a.remove_job(&job.id).await.unwrap();
    let replacement = a.add_flow_schedule_job("flow", daily()).await.unwrap();
    assert!(
        !a.complete_claim(&old, current.next_run, true, "replaced", None)
            .await
            .unwrap()
    );
    assert!(
        !a.renew_claim(&old, current.next_run, Duration::minutes(1))
            .await
            .unwrap()
    );
    assert!(!a.release_claim(&old, current.next_run).await.unwrap());
    assert!(a.list_runs(&replacement.id, 20).await.unwrap().is_empty());
    assert!(
        a.get_job(&replacement.id)
            .await
            .unwrap()
            .last_output
            .is_none()
    );
}

#[tokio::test]
async fn one_shot_claim_releases_admission_and_retains_delivery_history() {
    use tinyflows_schedule::{DeliveryStatus, Schedule};
    let a = super::test_support::store();
    a.set_max_active_jobs(Some(1)).await.unwrap();
    let at = chrono::Utc::now() + Duration::hours(1) + Duration::nanoseconds(333);
    let job = a
        .add_shell_job(None, Schedule::At { at }, "once")
        .await
        .unwrap();
    assert!(
        a.claim_due_job(
            &job.id,
            "w",
            at - Duration::nanoseconds(1),
            Duration::minutes(1)
        )
        .await
        .unwrap()
        .is_none()
    );
    let claim = a
        .claim_due_job(&job.id, "w", at, Duration::minutes(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claim.job().id, job.id);
    assert_eq!(claim.occurrence(), at);
    assert_eq!(claim.owner(), "w");
    assert!(!claim.token().is_empty());
    assert!(!claim.incarnation().is_empty());
    assert_eq!(claim.expires_at(), at + Duration::minutes(1));
    assert!(
        a.complete_claim(&claim, at, true, "sent", Some(DeliveryStatus::Delivered))
            .await
            .unwrap()
    );
    assert!(!a.get_job(&job.id).await.unwrap().enabled);
    assert!(
        a.claim_due_job(&job.id, "w", at, Duration::minutes(1))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        !a.complete_claim(&claim, at, true, "duplicate", None)
            .await
            .unwrap()
    );
    assert_eq!(a.list_runs(&job.id, 10).await.unwrap().len(), 1);
    a.add_shell_job(None, daily(), "next").await.unwrap();
}

#[tokio::test]
async fn release_and_restart_recovery_issue_fresh_tokens() {
    let backend = MemoryStorage::new();
    let a = CronDocuments::new(docs_in(&backend, "restart"));
    let job = a.add_shell_job(None, daily(), "echo").await.unwrap();
    let now = job.next_run;
    let first = a
        .claim_due_job(&job.id, "a", now, Duration::seconds(30))
        .await
        .unwrap()
        .unwrap();
    assert!(
        a.renew_claim(&first, now, Duration::seconds(60))
            .await
            .unwrap()
    );
    assert!(a.release_claim(&first, now).await.unwrap());
    assert!(!a.release_claim(&first, now).await.unwrap());
    let second = a
        .claim_due_job(&job.id, "a", now, Duration::seconds(30))
        .await
        .unwrap()
        .unwrap();
    assert_ne!(first.token(), second.token());
    drop(a);
    let b = CronDocuments::new(docs_in(&backend, "restart"));
    assert!(
        b.claim_due_job(
            &job.id,
            "b",
            now + Duration::seconds(29),
            Duration::seconds(30)
        )
        .await
        .unwrap()
        .is_none()
    );
    let third = b
        .claim_due_job(
            &job.id,
            "b",
            now + Duration::seconds(30),
            Duration::seconds(30),
        )
        .await
        .unwrap()
        .unwrap();
    assert_ne!(second.token(), third.token());
    assert!(
        b.complete_claim(&third, now + Duration::seconds(31), false, "failed", None)
            .await
            .unwrap()
    );
    assert_eq!(
        b.get_job(&job.id).await.unwrap().last_status.as_deref(),
        Some("error")
    );
}

#[tokio::test]
async fn invalid_inputs_and_malformed_stored_leases_fail_closed() {
    use serde_json::json;
    use tinystoragedrivers_core::Precondition;
    let a = super::test_support::store();
    let job = a.add_shell_job(None, daily(), "echo").await.unwrap();
    let now = job.next_run;
    assert!(
        a.claim_due_job(&job.id, "", now, Duration::seconds(1))
            .await
            .is_err()
    );
    assert!(
        a.claim_due_job(&job.id, "w", now, Duration::zero())
            .await
            .is_err()
    );
    assert!(
        a.claim_due_job(
            &job.id,
            "w",
            chrono::DateTime::<chrono::Utc>::MAX_UTC,
            Duration::seconds(1)
        )
        .await
        .is_err()
    );
    assert!(
        a.claim_due_job("missing", "w", now, Duration::seconds(1))
            .await
            .unwrap()
            .is_none()
    );
    let claim = a
        .claim_due_job(&job.id, "w", now, Duration::minutes(1))
        .await
        .unwrap()
        .unwrap();
    assert!(
        a.complete_claim(&claim, now - Duration::seconds(1), true, "bad", None)
            .await
            .is_err()
    );
    let mut stored = a.docs.get(super::JOBS, &job.id).await.unwrap().unwrap().doc;
    stored["occurrence_lease"] = json!({"token": 1});
    a.docs
        .put(super::JOBS, &job.id, stored, Precondition::None)
        .await
        .unwrap();
    assert!(
        a.claim_due_job(&job.id, "w", now, Duration::minutes(1))
            .await
            .is_err()
    );
    assert!(
        a.renew_claim(&claim, now, Duration::minutes(1))
            .await
            .is_err()
    );
    assert!(
        a.complete_claim(&claim, now, true, "bad", None)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn competing_claims_with_overlapping_snapshots_have_one_winner() {
    use super::test_support::FaultDocuments;
    use std::sync::{Arc, atomic::AtomicBool};
    let backend = MemoryStorage::new();
    let setup = CronDocuments::new(docs_in(&backend, "claim-overlap"));
    let job = setup.add_shell_job(None, daily(), "echo").await.unwrap();
    let gate = Arc::new(tokio::sync::Barrier::new(2));
    let handle = || {
        CronDocuments::new(Arc::new(FaultDocuments {
            inner: docs_in(&backend, "claim-overlap"),
            transactions: true,
            fail_batch: AtomicBool::new(false),
            batch_barrier: std::sync::Mutex::new(Some(gate.clone())),
        }))
    };
    let a = handle();
    let b = handle();
    let (first, second) = tokio::join!(
        a.claim_due_job(&job.id, "a", job.next_run, Duration::minutes(1)),
        b.claim_due_job(&job.id, "b", job.next_run, Duration::minutes(1))
    );
    assert_eq!(
        usize::from(first.unwrap().is_some()) + usize::from(second.unwrap().is_some()),
        1
    );
}
