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
        let job = update_job(
            &options,
            &job.id,
            CronJobPatch {
                delete_after_run: Some(remove),
                ..Default::default()
            },
        )
        .unwrap();
        assert!(claim_slot(&options, &job, job.next_run).unwrap());
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

#[test]
fn completion_preserves_auto_delete_canceled_during_execution() {
    let directory = tempfile::tempdir().unwrap();
    let options = CronStoreOptions::new(directory.path().join("jobs.db"));
    let job = add_shell_job(
        &options,
        None,
        Schedule::At {
            at: Utc::now() + chrono::Duration::hours(1),
        },
        "once",
    )
    .unwrap();
    let job = update_job(
        &options,
        &job.id,
        CronJobPatch {
            delete_after_run: Some(true),
            ..Default::default()
        },
    )
    .unwrap();
    assert!(claim_slot(&options, &job, job.next_run).unwrap());
    update_job(
        &options,
        &job.id,
        CronJobPatch {
            delete_after_run: Some(false),
            ..Default::default()
        },
    )
    .unwrap();
    complete_run(
        &options,
        &job,
        &Completion {
            remove: true,
            disable: false,
            next_run: None,
            finished_at: Utc::now(),
            success: true,
            output: "done".into(),
        },
    )
    .unwrap();
    let kept = get_job(&options, &job.id).unwrap();
    assert!(!kept.enabled);
    assert!(!kept.delete_after_run);
    assert_eq!(kept.last_output.as_deref(), Some("done"));
}

#[test]
fn completion_preserves_one_shot_reenabled_during_execution() {
    for (success, remove) in [(false, false), (true, false), (true, true)] {
        let directory = tempfile::tempdir().unwrap();
        let options = CronStoreOptions::new(directory.path().join("jobs.db"));
        let job = add_shell_job(
            &options,
            None,
            Schedule::At {
                at: Utc::now() + chrono::Duration::hours(1),
            },
            "once",
        )
        .unwrap();
        let job = update_job(
            &options,
            &job.id,
            CronJobPatch {
                delete_after_run: Some(remove),
                ..Default::default()
            },
        )
        .unwrap();
        assert!(claim_slot(&options, &job, job.next_run).unwrap());
        update_job(
            &options,
            &job.id,
            CronJobPatch {
                enabled: Some(true),
                ..Default::default()
            },
        )
        .unwrap();
        let completion = Completion {
            remove,
            disable: !remove,
            next_run: None,
            finished_at: Utc::now(),
            success,
            output: "done".into(),
        };
        complete_run(&options, &job, &completion).unwrap();
        let kept = get_job(&options, &job.id).unwrap();
        assert!(kept.enabled, "completion overwrote the explicit re-enable");
        assert_eq!(kept.last_output.as_deref(), Some("done"));
    }
}

#[test]
fn due_selection_replaces_an_excluded_head_before_applying_the_batch_limit() {
    let directory = tempfile::tempdir().unwrap();
    let mut options = CronStoreOptions::new(directory.path().join("jobs.db"));
    options.max_tasks = 1;
    let command = "first";
    let first = add_shell_job(
        &options,
        None,
        Schedule::Every { every_ms: 60_000 },
        command,
    )
    .unwrap();
    let command = "second";
    let second = add_shell_job(
        &options,
        None,
        Schedule::Every { every_ms: 60_000 },
        command,
    )
    .unwrap();
    let now = second.next_run + chrono::Duration::days(1);
    let due = super::super::due_jobs_excluding(&options, now, &[first.id.clone()]).unwrap();
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].id, second.id);
}
