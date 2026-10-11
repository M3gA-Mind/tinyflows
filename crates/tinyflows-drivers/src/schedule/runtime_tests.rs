use super::super::test_support::store;
use super::*;
use tinyflows_schedule::{CronJobPatch, Schedule};
#[tokio::test]
async fn completion_keeps_a_schedule_edited_after_the_slot_was_claimed() {
    let store = store();
    let job = store
        .add_shell_job(
            Some("seed".into()),
            Schedule::Every { every_ms: 60000 },
            "echo seed",
        )
        .await
        .unwrap();
    assert!(
        store
            .claim_slot(&job, job.next_run + chrono::Duration::seconds(60))
            .await
            .unwrap()
    );
    assert!(
        !store
            .claim_slot(&job, job.next_run + chrono::Duration::seconds(60))
            .await
            .unwrap()
    );
    let edited = store
        .update_job(
            &job.id,
            CronJobPatch {
                schedule: Some(Schedule::Every { every_ms: 300000 }),
                enabled: Some(false),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    store
        .complete_run(
            &job,
            &Completion {
                remove: false,
                disable: false,
                next_run: Some(job.next_run + chrono::Duration::hours(1)),
                finished_at: Utc::now(),
                success: true,
                output: "finished".into(),
            },
        )
        .await
        .unwrap();
    let kept = store.get_job(&job.id).await.unwrap();
    assert_eq!(kept.schedule, edited.schedule);
    assert_eq!(kept.next_run, edited.next_run);
    assert!(!kept.enabled);
    assert_eq!(kept.last_output.as_deref(), Some("finished"));
}
#[tokio::test]
async fn completed_one_shot_is_disabled_and_successful_auto_delete_is_idempotent() {
    let store = store();
    for remove in [false, true] {
        let job = store
            .add_shell_job(
                None,
                Schedule::At {
                    at: Utc::now() + chrono::Duration::hours(1),
                },
                "echo once",
            )
            .await
            .unwrap();
        let completion = Completion {
            remove,
            disable: !remove,
            next_run: None,
            finished_at: Utc::now(),
            success: true,
            output: "done".into(),
        };
        store.complete_run(&job, &completion).await.unwrap();
        store.complete_run(&job, &completion).await.unwrap();
        if remove {
            assert!(store.get_job(&job.id).await.is_err());
        } else {
            assert!(!store.get_job(&job.id).await.unwrap().enabled);
        }
    }
}
