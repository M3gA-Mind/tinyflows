use super::*;

#[test]
fn terminal_payload_uses_the_engine_success_vocabulary() {
    for (status, successful) in [
        ("completed", true),
        ("completed_with_warnings", true),
        ("failed", false),
        ("cancelled", false),
        ("interrupted", false),
        ("future_terminal_status", false),
    ] {
        let payload = finished_payload(status, None, None, None, false);
        assert_eq!(payload.status, status);
        assert_eq!(payload.successful, successful, "status: {status}");
    }
}

#[test]
fn terminal_payload_preserves_engine_output_and_deadline_cause() {
    let output = serde_json::json!({"partial":true});
    let payload = finished_payload("cancelled", None, None, Some(output.clone()), false);
    assert_eq!(payload.output, Some(output));
    assert!(!payload.timed_out);

    let timeout = finished_payload("cancelled", Some("deadline".into()), None, None, true);
    assert_eq!(timeout.output, None);
    assert!(timeout.timed_out);
}

#[test]
fn resumed_node_replaces_its_parked_step_instead_of_retaining_stale_diagnostics() {
    let steps = Mutex::new(vec![FlowRunStep {
        node_id: "approval".into(),
        status: Some("success".into()),
        diagnostics: vec![serde_json::json!({"location":"parked-at-gate"})],
        ..Default::default()
    }]);

    record_step(
        &steps,
        FlowRunStep {
            node_id: "approval".into(),
            status: Some("success".into()),
            diagnostics: Vec::new(),
            output: serde_json::json!([{"json":{"approved":true}}]),
            ..Default::default()
        },
    );

    let steps = steps.into_inner().unwrap();
    assert_eq!(steps.len(), 1);
    assert_eq!(steps[0].status.as_deref(), Some("success"));
    assert!(steps[0].diagnostics.is_empty());
    assert_eq!(
        steps[0].output,
        serde_json::json!([{"json":{"approved":true}}])
    );
}
