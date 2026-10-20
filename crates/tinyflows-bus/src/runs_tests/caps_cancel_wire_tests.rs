use super::*;

#[test]
fn agent_cancel_request_uses_the_matching_request_id_wire_shape() {
    pin(
        AgentCancelRequest {
            client_request_id: "request-1".into(),
        },
        serde_json::json!({"client_request_id":"request-1"}),
    );
}

#[test]
fn legacy_agent_run_requests_default_to_an_empty_cancellation_id() {
    let legacy: AgentRunRequest = serde_json::from_value(serde_json::json!({
        "agent":{"id":"legacy"},
        "config":null
    }))
    .unwrap();
    assert_eq!(legacy.client_request_id, "");
}
