use super::*;
use serde_json::{Value, json};

fn graph_with_trigger(config: Value) -> WorkflowGraph {
    serde_json::from_value(json!({
        "name": "t",
        "nodes": [{ "id": "t", "kind": "trigger", "name": "Trigger", "config": config }],
        "edges": []
    }))
    .expect("graph")
}

#[test]
fn labels_match_the_serde_discriminator() {
    for kind in [
        TriggerKind::Manual,
        TriggerKind::Schedule,
        TriggerKind::Webhook,
        TriggerKind::AppEvent,
        TriggerKind::Form,
        TriggerKind::ExecuteByWorkflow,
        TriggerKind::ChatMessage,
        TriggerKind::Evaluation,
        TriggerKind::System,
    ] {
        assert_eq!(
            serde_json::to_value(&kind).unwrap(),
            json!(trigger_kind_label(&kind))
        );
    }
}

#[test]
fn only_manual_schedule_and_app_event_fire() {
    assert!(trigger_kind_fires(&TriggerKind::Manual));
    assert!(trigger_kind_fires(&TriggerKind::Schedule));
    assert!(trigger_kind_fires(&TriggerKind::AppEvent));
    assert!(!trigger_kind_fires(&TriggerKind::Webhook));
    assert!(!trigger_kind_fires(&TriggerKind::Form));
}

#[test]
fn unfired_kind_warns_and_names_the_kind() {
    let warnings = graph_trigger_warnings(&graph_with_trigger(json!({"trigger_kind": "webhook"})));
    assert_eq!(warnings.len(), 1);
    assert!(warnings[0].contains("'webhook'"));
    assert!(warnings[0].contains("does not fire automatically"));
}

#[test]
fn fired_legacy_and_unknown_kinds_do_not_warn() {
    assert!(
        graph_trigger_warnings(&graph_with_trigger(json!({"trigger_kind": "schedule"}))).is_empty()
    );
    assert!(graph_trigger_warnings(&graph_with_trigger(json!({}))).is_empty());
    assert!(
        graph_trigger_warnings(&graph_with_trigger(json!({"trigger_kind": "bogus"}))).is_empty()
    );
}

#[test]
fn host_supplied_firing_set_overrides_the_default() {
    let graph = graph_with_trigger(json!({"trigger_kind": "webhook"}));
    assert!(graph_trigger_warnings_for(&graph, |_| true).is_empty());
}

#[test]
fn app_event_binding_is_single_trigger_and_case_insensitive() {
    let graph = graph_with_trigger(json!({
        "trigger_kind":"app_event", "toolkit":"GitHub", "trigger_slug":"ISSUE_CREATED"
    }));
    assert!(app_event_binding(&graph, "github", "issue_created"));
    assert!(!app_event_binding(&graph, "slack", "issue_created"));
    assert!(!app_event_binding(&graph, "github", "issue_updated"));

    let multiple = serde_json::from_value(json!({
        "name":"t", "nodes":[
            {"id":"a","kind":"trigger","name":"A","config":{"trigger_kind":"app_event","toolkit":"GitHub","trigger_slug":"ISSUE_CREATED"}},
            {"id":"b","kind":"trigger","name":"B","config":{"trigger_kind":"app_event","toolkit":"GitHub","trigger_slug":"ISSUE_CREATED"}}
        ], "edges":[]
    })).unwrap();
    assert!(!app_event_binding(&multiple, "GitHub", "ISSUE_CREATED"));
}

#[test]
fn pinned_inputs_are_literal_objects_and_invalid_shapes_are_empty() {
    let graph = graph_with_trigger(json!({
        "trigger_kind":"app_event", "inputs":{"value":"=1 + 1", "object":{"x":1}}
    }));
    assert_eq!(pinned_inputs(&graph)["value"], "=1 + 1");
    assert_eq!(pinned_inputs(&graph)["object"], json!({"x":1}));
    assert!(pinned_inputs(&graph_with_trigger(json!({"inputs":[]}))).is_empty());
}
