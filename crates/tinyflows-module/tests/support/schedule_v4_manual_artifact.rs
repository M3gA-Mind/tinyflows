use super::callbacks;
use chrono::{Duration, Utc};
use serde_json::Value;
use std::sync::Arc;
use tinybus::Proxy;
use tinyflows_bus::schedule::{
    CallbackAddress, ManualDelivery, RunJobNow, RunJobRefusal, RunJobReply, StoreConfig,
};
use tinyflows_drivers::schedule::CronDocuments;
use tinyflows_schedule::{CronJobPatch, Schedule};
use tinystoragedrivers_core::DocumentStore;

/// Exercises module-owned manual queueing and lease refusal through the loaded cdylib.
pub async fn verify(
    proxy: &Proxy,
    docs: Arc<dyn DocumentStore>,
    storage: Value,
    callback: CallbackAddress,
    state: Arc<callbacks::State>,
) {
    let store = CronDocuments::new(docs);
    let now = Utc::now();
    let saved = store
        .add_shell_job(
            Some("artifact-manual".into()),
            Schedule::At {
                at: now + Duration::hours(1),
            },
            "echo manual",
        )
        .await
        .unwrap();
    let saved = store
        .update_job(
            &saved.id,
            CronJobPatch {
                enabled: Some(false),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let storage: StoreConfig = serde_json::from_value(storage.clone()).unwrap();
    let reply: RunJobReply = proxy
        .call(
            "RunJobNow",
            &(RunJobNow {
                scope: "artifact-manual-scope".into(),
                callback: callback.clone(),
                config: serde_json::json!({}),
                storage: storage.clone(),
                job: saved.clone(),
                run_id: "artifact-manual-run".into(),
                delivery: ManualDelivery::NotRequested,
                wait: true,
            },),
        )
        .await
        .unwrap();
    let RunJobReply::Completed {
        run_id,
        success,
        delivery_status,
        ..
    } = reply
    else {
        panic!("loaded module must finish a blocking manual run")
    };
    assert_eq!(run_id, "artifact-manual-run");
    assert!(success);
    assert_eq!(delivery_status, None);
    let after = store.get_job(&saved.id).await.unwrap();
    assert!(!after.enabled);
    assert_eq!(after.next_run, saved.next_run);
    assert_eq!(after.delete_after_run, saved.delete_after_run);
    let history = store.list_runs(&saved.id, 10).await.unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].status, "success");
    assert_eq!(history[0].delivery_status, None);
    assert!(state
        .executions
        .lock()
        .unwrap()
        .contains(&"artifact-manual-run".to_owned()));

    let held_job = store
        .add_shell_job(
            None,
            Schedule::At {
                at: Utc::now() + Duration::hours(1),
            },
            "echo held",
        )
        .await
        .unwrap();
    let held = store
        .claim_job_now(&held_job, "scheduled-owner", Utc::now(), Duration::hours(2))
        .await
        .unwrap()
        .unwrap();
    let refusal: RunJobReply = proxy
        .call(
            "RunJobNow",
            &(RunJobNow {
                scope: "artifact-manual-scope".into(),
                callback,
                config: serde_json::json!({}),
                storage,
                job: held_job.clone(),
                run_id: "artifact-refused-run".into(),
                delivery: ManualDelivery::NotRequested,
                wait: true,
            },),
        )
        .await
        .unwrap();
    assert!(matches!(
        refusal,
        RunJobReply::Refused {
            reason: RunJobRefusal::AlreadyRunning
        }
    ));
    assert!(store.list_runs(&held_job.id, 10).await.unwrap().is_empty());
    assert!(store.release_claim(&held, Utc::now()).await.unwrap());
}
