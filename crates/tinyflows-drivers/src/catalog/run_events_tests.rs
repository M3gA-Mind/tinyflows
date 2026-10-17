use super::*;
use crate::catalog::test_support::{catalog, trigger_graph};
use serde_json::json;

#[tokio::test]
async fn run_events_append_in_order_and_poll_after_cursor() {
    let catalog = catalog();
    let flow = catalog
        .create_flow("Events".into(), trigger_graph(), false, true)
        .await
        .unwrap();
    catalog
        .insert_flow_run("event-run", &flow.id, "event-run", "2026-01-01T00:00:00Z")
        .await
        .unwrap();

    for seq in 1..=3 {
        catalog
            .append_flow_run_event(
                "event-run",
                json!({"seq":seq,"kind":"step","data":{"node":seq}}),
            )
            .await
            .unwrap();
    }
    assert_eq!(
        catalog.flow_run_event_last_seq("event-run").await.unwrap(),
        3
    );
    assert_eq!(
        catalog
            .flow_run_events_after("event-run", 1, 2)
            .await
            .unwrap(),
        [
            json!({"seq":2,"kind":"step","data":{"node":2}}),
            json!({"seq":3,"kind":"step","data":{"node":3}})
        ]
    );
}

#[tokio::test]
async fn run_event_retry_is_idempotent_and_sequence_gaps_are_rejected() {
    let catalog = catalog();
    let flow = catalog
        .create_flow("Events".into(), trigger_graph(), false, true)
        .await
        .unwrap();
    catalog
        .insert_flow_run("event-run", &flow.id, "event-run", "2026-01-01T00:00:00Z")
        .await
        .unwrap();

    let event = json!({"seq":1,"kind":"step","data":{}});
    catalog
        .append_flow_run_event("event-run", event.clone())
        .await
        .unwrap();
    catalog
        .append_flow_run_event("event-run", event)
        .await
        .unwrap();
    assert!(
        catalog
            .append_flow_run_event("event-run", json!({"seq":1,"kind":"conflict"}))
            .await
            .is_err()
    );
    catalog
        .append_flow_run_event("event-run", json!({"seq":2,"kind":"step","data":{}}))
        .await
        .unwrap();
    catalog
        .append_flow_run_event("event-run", json!({"seq":1,"kind":"step","data":{}}))
        .await
        .unwrap();
    assert!(
        catalog
            .append_flow_run_event("event-run", json!({"seq":4,"kind":"step"}))
            .await
            .is_err()
    );
    assert_eq!(
        catalog.flow_run_event_last_seq("event-run").await.unwrap(),
        2
    );
}

#[tokio::test]
async fn terminal_cursor_is_marked_only_after_terminal_event_is_durable() {
    let catalog = catalog();
    let flow = catalog
        .create_flow("Events".into(), trigger_graph(), false, true)
        .await
        .unwrap();
    catalog
        .insert_flow_run("event-run", &flow.id, "event-run", "2026-01-01T00:00:00Z")
        .await
        .unwrap();
    catalog
        .finish_flow_run(
            "event-run",
            "completed",
            "2026-01-01T00:00:01Z",
            &[],
            &[],
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        catalog
            .flow_run_terminal_event_seq("event-run")
            .await
            .unwrap(),
        None
    );

    catalog
        .append_flow_run_event(
            "event-run",
            json!({"seq":1,"kind":"step","data":{"status":"completed"}}),
        )
        .await
        .unwrap();
    assert_eq!(
        catalog
            .flow_run_terminal_event_seq("event-run")
            .await
            .unwrap(),
        None,
        "the event itself must be durable before its terminal marker"
    );
    assert!(
        !catalog
            .mark_flow_run_terminal_event("event-run", 1)
            .await
            .unwrap()
    );
    catalog
        .append_flow_run_event(
            "event-run",
            json!({"seq":2,"kind":"finished","data":{"status":"completed"}}),
        )
        .await
        .unwrap();
    assert!(
        catalog
            .mark_flow_run_terminal_event("event-run", 2)
            .await
            .unwrap()
    );
    assert_eq!(
        catalog
            .flow_run_terminal_event_seq("event-run")
            .await
            .unwrap(),
        Some(2)
    );
    catalog
        .append_flow_run_event(
            "event-run",
            json!({"seq":3,"kind":"finished","data":{"status":"failed"}}),
        )
        .await
        .unwrap();
    assert!(
        !catalog
            .mark_flow_run_terminal_event("event-run", 3)
            .await
            .unwrap(),
        "terminal event status must match the settled row"
    );
}
