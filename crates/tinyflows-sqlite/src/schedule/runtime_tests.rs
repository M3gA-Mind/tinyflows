use super::super::{add_shell_job, get_job, update_job};
use super::*;
use tinyflows_schedule::{CronJobPatch, Schedule};
#[test]
fn completion_keeps_a_schedule_edited_after_the_slot_was_claimed() {
    let directory = tempfile::tempdir().unwrap();
    let options = CronStoreOptions::new(directory.path().join("jobs.db"));
    let job = add_shell_job(
        &options,
        Some("seed".into()),
        Schedule::Every { every_ms: 60000 },
        "echo seed",
    )
    .unwrap();
    assert!(claim_slot(&options, &job, job.next_run + chrono::Duration::seconds(60)).unwrap());
    assert!(!claim_slot(&options, &job, job.next_run + chrono::Duration::seconds(60)).unwrap());
    let edited = update_job(
        &options,
        &job.id,
        CronJobPatch {
            schedule: Some(Schedule::Every { every_ms: 300000 }),
            enabled: Some(false),
            ..Default::default()
        },
    )
    .unwrap();
    complete_run(
        &options,
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
    .unwrap();
    let kept = get_job(&options, &job.id).unwrap();
    assert_eq!(kept.schedule, edited.schedule);
    assert_eq!(kept.next_run, edited.next_run);
    assert!(!kept.enabled);
    assert_eq!(kept.last_output.as_deref(), Some("finished"));
}
#[test]
fn completed_one_shot_is_disabled_and_successful_auto_delete_is_atomic() {
    let directory = tempfile::tempdir().unwrap();
    let options = CronStoreOptions::new(directory.path().join("jobs.db"));
    for remove in [false, true] {
        let job = add_shell_job(
            &options,
            None,
            Schedule::At {
                at: Utc::now() + chrono::Duration::hours(1),
            },
            "echo once",
        )
        .unwrap();
        let completion = Completion {
            remove,
            disable: !remove,
            next_run: None,
            finished_at: Utc::now(),
            success: true,
            output: "done".into(),
        };
        complete_run(&options, &job, &completion).unwrap();
        complete_run(&options, &job, &completion).unwrap();
        if remove {
            assert!(get_job(&options, &job.id).is_err());
        } else {
            assert!(!get_job(&options, &job.id).unwrap().enabled);
        }
    }
}

#[test]
fn one_shot_claim_is_durable_without_history_or_completion() {
    let directory = tempfile::tempdir().unwrap();
    let options = CronStoreOptions::new(directory.path().join("jobs.db"));
    let job = add_shell_job(
        &options,
        None,
        Schedule::At {
            at: Utc::now() + chrono::Duration::seconds(1),
        },
        "once",
    )
    .unwrap();
    assert!(claim_slot(&options, &job, job.next_run).unwrap());
    assert!(!get_job(&options, &job.id).unwrap().enabled);
    assert!(!claim_slot(&options, &job, job.next_run).unwrap());
    assert!(
        super::super::due_jobs(&options, job.next_run)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn claim_rejects_an_execution_definition_edited_after_due_selection() {
    let directory = tempfile::tempdir().unwrap();
    let options = CronStoreOptions::new(directory.path().join("jobs.db"));
    let job = add_shell_job(
        &options,
        None,
        Schedule::Every { every_ms: 60000 },
        "old command",
    )
    .unwrap();
    let edited = update_job(
        &options,
        &job.id,
        CronJobPatch {
            command: Some("new command".into()),
            ..Default::default()
        },
    )
    .unwrap();
    assert!(!claim_slot(&options, &job, job.next_run + chrono::Duration::minutes(1)).unwrap());
    assert_eq!(
        get_job(&options, &job.id).unwrap().next_run,
        edited.next_run
    );
    assert!(
        claim_slot(
            &options,
            &edited,
            edited.next_run + chrono::Duration::minutes(1)
        )
        .unwrap()
    );
}
