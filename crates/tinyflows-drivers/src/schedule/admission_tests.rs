//! Shared admission policy fences both configured and existing handles.
use super::{
    CronDocuments,
    test_support::{daily, docs_in},
};
use tinyflows_schedule::CronJobPatch;
use tinystoragedrivers_core::MemoryStorage;

#[tokio::test]
async fn an_existing_unlimited_handle_cannot_bypass_installed_policy() {
    let backend = MemoryStorage::new();
    let a = CronDocuments::new(docs_in(&backend, "admission"));
    let b = CronDocuments::new(docs_in(&backend, "admission"));
    a.set_max_active_jobs(Some(1)).await.unwrap();
    let (first, second) = tokio::join!(
        a.add_shell_job(None, daily(), "a"),
        b.add_shell_job(None, daily(), "b")
    );
    assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
    let job = a.list_jobs().await.unwrap().pop().unwrap();
    a.update_job(
        &job.id,
        CronJobPatch {
            enabled: Some(false),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let next = b.add_shell_job(None, daily(), "next").await.unwrap();
    assert!(
        a.update_job(
            &job.id,
            CronJobPatch {
                enabled: Some(true),
                ..Default::default()
            }
        )
        .await
        .is_err()
    );
    b.remove_job(&next.id).await.unwrap();
    a.update_job(
        &job.id,
        CronJobPatch {
            enabled: Some(true),
            ..Default::default()
        },
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn policy_installation_counts_existing_jobs_and_refuses_a_lower_cap() {
    let a = super::test_support::store();
    a.add_shell_job(None, daily(), "a").await.unwrap();
    a.add_shell_job(None, daily(), "b").await.unwrap();
    assert!(a.set_max_active_jobs(Some(1)).await.is_err());
    a.set_max_active_jobs(Some(2)).await.unwrap();
    assert!(a.add_shell_job(None, daily(), "c").await.is_err());
    a.clear_all_jobs().await.unwrap();
    a.add_shell_job(None, daily(), "c").await.unwrap();
}
