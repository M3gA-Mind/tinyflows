use super::*;
use serde_json::json;
#[path = "events_persistence_test_support.rs"]
pub(super) mod support;
use support::{FaultMode, poll, setup};

#[tokio::test]
async fn oversized_terminal_output_falls_back_to_one_pollable_finished_event() {
    let setup = setup(
        FaultMode::LimitOutcomeDocument(4_096),
        "completed_with_warnings",
    )
    .await;
    let output = json!({"body":"x".repeat(8_192)});
    let writer = EventWriter::new(setup.catalog.clone(), setup.run_id.clone(), 0);
    let result = writer
        .emit_terminal(finished_payload(
            "completed_with_warnings",
            Some("engine warning".into()),
            None,
            Some(output),
            false,
        ))
        .await;
    match &result {
        Ok(seq) => {
            assert_eq!(*seq, 1);
            assert_eq!(writer.flush().await, None);
            assert!(
                setup
                    .catalog
                    .mark_flow_run_terminal_event(&setup.run_id, *seq)
                    .await
                    .unwrap()
            );
        }
        Err(_) => {
            assert!(writer.flush().await.is_some());
        }
    }
    let page = poll(&setup).await;
    assert!(
        page.done,
        "terminal run stopped polling after the output-bearing event hit the size limit: {result:?}"
    );
    assert_eq!(page.events.len(), 1);
    let finished: tinyflows_bus::runs::RunFinished =
        serde_json::from_value(page.events[0].data.clone()).unwrap();
    assert_eq!(finished.status, "completed_with_warnings");
    assert!(finished.successful);
    assert_eq!(finished.error.as_deref(), Some("engine warning"));
    assert_eq!(finished.output, None);
    assert!(!finished.timed_out);
}

#[tokio::test]
async fn oversized_parked_output_preserves_approval_metadata_and_remains_nonterminal() {
    let setup = setup(FaultMode::LimitOutcomeDocument(4_096), "pending_approval").await;
    let writer = EventWriter::new(setup.catalog.clone(), setup.run_id.clone(), 0);
    writer.emit(
        "parked",
        serde_json::to_value(tinyflows_bus::runs::RunParked {
            output: Some(json!({"body":"x".repeat(8_192)})),
            pending_approvals: vec!["approve-node".into()],
            graph_hash: Some("pinned-graph".into()),
        })
        .unwrap(),
    );

    assert_eq!(writer.flush().await, None);
    let page = poll(&setup).await;
    assert!(!page.done, "parked runs remain nonterminal");
    assert_eq!(page.events.len(), 1);
    assert_eq!(page.events[0].kind, "parked");
    let parked: tinyflows_bus::runs::RunParked =
        serde_json::from_value(page.events[0].data.clone()).unwrap();
    assert_eq!(parked.output, None);
    assert_eq!(parked.pending_approvals, ["approve-node"]);
    assert_eq!(parked.graph_hash.as_deref(), Some("pinned-graph"));
}

#[tokio::test]
async fn output_free_fallback_lost_ack_is_reconciled_without_duplicate_event() {
    let setup = setup(
        FaultMode::LimitOutputThenLoseFallbackAck(4_096),
        "completed_with_warnings",
    )
    .await;
    let writer = EventWriter::new(setup.catalog.clone(), setup.run_id.clone(), 0);
    let result = writer
        .emit_terminal(finished_payload(
            "completed_with_warnings",
            Some("warning preserved".into()),
            None,
            Some(json!({"body":"x".repeat(8_192)})),
            false,
        ))
        .await;
    match &result {
        Ok(seq) => assert!(
            setup
                .catalog
                .mark_flow_run_terminal_event(&setup.run_id, *seq)
                .await
                .unwrap()
        ),
        Err(_) => assert!(writer.flush().await.is_some()),
    }

    let page = poll(&setup).await;
    assert!(
        page.done,
        "committed output-free fallback was not reconciled after lost acknowledgement: {result:?}"
    );
    assert_eq!(page.events.len(), 1);
    let finished: tinyflows_bus::runs::RunFinished =
        serde_json::from_value(page.events[0].data.clone()).unwrap();
    assert_eq!(finished.output, None);
    assert_eq!(finished.error.as_deref(), Some("warning preserved"));
    assert_eq!(finished.status, "completed_with_warnings");
}

#[tokio::test]
async fn late_original_terminal_commit_wins_over_the_output_free_fallback() {
    let setup = setup(
        FaultMode::RejectOutputThenOriginalWins,
        "completed_with_warnings",
    )
    .await;
    let output = json!({"kept":"small outcome"});
    let writer = EventWriter::new(setup.catalog.clone(), setup.run_id.clone(), 0);
    let seq = writer
        .emit_terminal(finished_payload(
            "completed_with_warnings",
            Some("warning preserved".into()),
            None,
            Some(output.clone()),
            false,
        ))
        .await
        .expect("the matching original event is accepted after it wins the sequence race");

    assert_eq!(seq, 1);
    assert_eq!(writer.flush().await, None);
    assert!(
        setup
            .catalog
            .mark_flow_run_terminal_event(&setup.run_id, seq)
            .await
            .unwrap()
    );
    let page = poll(&setup).await;
    assert!(page.done);
    assert_eq!(page.events.len(), 1);
    let finished: tinyflows_bus::runs::RunFinished =
        serde_json::from_value(page.events[0].data.clone()).unwrap();
    assert_eq!(finished.output, Some(output));
}

#[tokio::test]
async fn terminal_reconciliation_rejects_different_payload_at_the_same_sequence() {
    let setup = setup(FaultMode::LimitOutcomeDocument(4_096), "completed").await;
    let different = json!({"status":"failed","successful":false});
    setup
        .catalog
        .append_flow_run_event(
            &setup.run_id,
            json!({"seq":1,"kind":"finished","data":different,"graph_run_id":null}),
        )
        .await
        .unwrap();
    let expected = json!({"status":"completed","successful":true});

    let state =
        super::outcome_event_state(&setup.catalog, &setup.run_id, 1, "finished", &[&expected])
            .await
            .unwrap();

    assert_eq!(state, super::OutcomeEventState::Conflicting);
}

#[tokio::test]
async fn terminal_event_lost_ack_retries_the_exact_payload_without_duplication() {
    let setup = setup(FaultMode::CommitThenLoseAck, "completed").await;
    let output = json!({"kept":true});
    let writer = EventWriter::new(setup.catalog.clone(), setup.run_id.clone(), 0);
    let seq = writer
        .emit_terminal(finished_payload(
            "completed",
            None,
            None,
            Some(output.clone()),
            false,
        ))
        .await
        .expect("retry should recognize the identical event already committed");

    assert_eq!(seq, 1);
    assert_eq!(writer.flush().await, None);
    assert!(
        setup
            .catalog
            .mark_flow_run_terminal_event(&setup.run_id, seq)
            .await
            .unwrap()
    );
    let page = poll(&setup).await;
    assert!(page.done);
    assert_eq!(page.events.len(), 1);
    let finished: tinyflows_bus::runs::RunFinished =
        serde_json::from_value(page.events[0].data.clone()).unwrap();
    assert_eq!(finished.output, Some(output));
}

#[tokio::test]
async fn recovered_terminal_retry_preserves_an_earlier_event_writer_error() {
    let setup = setup(FaultMode::CommitThenLoseAck, "completed").await;
    let mut seq = 0;
    let mut last_error = Some("earlier progress persistence failed".to_string());
    let data = serde_json::to_value(finished_payload(
        "completed",
        None,
        None,
        Some(json!({"kept":true})),
        false,
    ))
    .unwrap();

    let terminal_seq = persist_outcome_event(
        &setup.catalog,
        &setup.run_id,
        &mut seq,
        "finished",
        data,
        &mut last_error,
    )
    .await
    .unwrap();

    assert_eq!(terminal_seq, 1);
    assert_eq!(seq, 1);
    assert_eq!(
        last_error.as_deref(),
        Some("earlier progress persistence failed")
    );
}

#[tokio::test]
async fn terminal_event_stops_cleanly_when_output_and_fallback_writes_fail() {
    let setup = setup(FaultMode::RejectEveryTerminalWrite, "failed").await;
    let writer = EventWriter::new(setup.catalog.clone(), setup.run_id.clone(), 0);
    let error = writer
        .emit_terminal(finished_payload(
            "failed",
            Some("engine failed".into()),
            None,
            Some(json!({"body":"x".repeat(8_192)})),
            false,
        ))
        .await
        .expect_err("backend failures must remain visible to the actor");

    assert!(
        error
            .to_string()
            .contains("injected terminal write failure")
    );
    assert!(writer.flush().await.is_some());
    let page = poll(&setup).await;
    assert!(!page.done);
    assert!(page.events.is_empty());
}
