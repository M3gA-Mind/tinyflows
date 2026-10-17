//! Manual passes use one supplied calendar instant across dispatch and records.

use super::*;
use chrono::{Duration, Utc};
use serde_json::json;

#[tokio::test]
async fn a_manual_pass_uses_the_supplied_time_for_due_jobs_and_run_records() {
    let (_server, host, state) = tests::connections().await;
    let proxy = host
        .proxy(
            schedule::BUS_NAME,
            schedule::OBJECT_PATH,
            schedule::INTERFACE,
        )
        .unwrap();
    let now = Utc::now() + Duration::days(1);
    let mut job = tests::job("shell");
    job["schedule"] = json!({"kind":"at", "at": now});
    job["next_run"] = json!(now);
    state.jobs.lock().await.push(job);
    let mut repeating = tests::job("shell");
    repeating["id"] = json!("repeat");
    repeating["next_run"] = json!(now);
    state.jobs.lock().await.push(repeating);

    // The exact same future job is not due before the supplied instant.
    for (at, expected) in [(now - Duration::seconds(1), 0), (now, 2)] {
        let request = json!({
            "scope": "fixed-clock", "callback": tests::callback(),
            "manual_ticks": true, "config": {"now": at},
        });
        let handle: String = proxy
            .call(schedule::members::START_SCHEDULER, &(request,))
            .await
            .unwrap();
        let count: usize = proxy
            .call(schedule::members::TICK_SCHEDULER, &(&handle,))
            .await
            .unwrap();
        assert_eq!(count, expected);
        proxy
            .call::<()>(schedule::members::DRAIN_SCHEDULER, &(&handle,))
            .await
            .unwrap();
        proxy
            .call::<()>(schedule::members::STOP_SCHEDULER, &(&handle,))
            .await
            .unwrap();
    }
    let times = state.observed_times.lock().await;
    assert!(times.contains(&(now - Duration::seconds(1))));
    assert!(times.contains(&now));
    let records = state.records.lock().await;
    assert_eq!(records.len(), 2);
    for record in records.iter() {
        assert_eq!(record["started_at"], json!(now));
        assert_eq!(record["finished_at"], json!(now));
    }
    assert!(
        state
            .completions
            .lock()
            .await
            .iter()
            .any(|completion| { completion["next_run"] == json!(now + Duration::seconds(60)) })
    );
}

#[tokio::test]
async fn a_fixed_calendar_time_is_refused_for_an_automatic_loop() {
    let (_server, host, _) = tests::connections().await;
    let proxy = host
        .proxy(
            schedule::BUS_NAME,
            schedule::OBJECT_PATH,
            schedule::INTERFACE,
        )
        .unwrap();
    let request = json!({
        "scope": "fixed-loop", "callback": tests::callback(),
        "config": {"now": Utc::now()},
    });
    let error = proxy
        .call::<String>(schedule::members::START_SCHEDULER, &(request,))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("manual ticks"), "{error}");
}

#[tokio::test]
async fn an_invalid_calendar_time_is_refused() {
    let (_server, host, _) = tests::connections().await;
    let proxy = host
        .proxy(
            schedule::BUS_NAME,
            schedule::OBJECT_PATH,
            schedule::INTERFACE,
        )
        .unwrap();
    for now in [json!("not a date"), json!(42), Value::Null] {
        let request = json!({
            "scope": "invalid-clock", "callback": tests::callback(),
            "manual_ticks": true, "config": {"now": now},
        });
        assert!(
            proxy
                .call::<String>(schedule::members::START_SCHEDULER, &(request,))
                .await
                .is_err()
        );
    }
}
