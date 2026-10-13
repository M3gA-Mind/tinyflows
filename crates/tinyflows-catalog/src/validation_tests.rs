use super::*;
use serde_json::json;

#[test]
fn structural_error_keeps_code_message_and_node() {
    let graph: WorkflowGraph = serde_json::from_value(json!({
        "name": "x",
        "nodes": [{ "id": "a", "kind": "transform", "name": "A" }],
        "edges": []
    }))
    .unwrap();
    let errors = tinyflows::validate::validate_all(&graph);
    assert!(!errors.is_empty());
    let mapped = to_flow_validation_error(&errors[0]);
    assert_eq!(mapped.code, errors[0].code());
    assert_eq!(mapped.message, errors[0].to_string());
    assert_eq!(mapped.node_id.as_deref(), errors[0].node_id());
    assert_eq!(mapped.field, None);
}

#[test]
fn compat_error_maps_field_for_field() {
    let mapped = to_compat_validation_error(tinyflows::compat::CompatibilityError {
        code: "some_code",
        message: "msg".into(),
        node_id: Some("n".into()),
    });
    assert_eq!(mapped.code, "some_code");
    assert_eq!(mapped.message, "msg");
    assert_eq!(mapped.node_id.as_deref(), Some("n"));
}

#[test]
fn simple_graph_has_no_engine_compatibility_errors() {
    let graph: WorkflowGraph = serde_json::from_value(json!({
        "name": "x",
        "nodes": [{ "id": "t", "kind": "trigger", "name": "T" }],
        "edges": []
    }))
    .unwrap();
    assert!(engine_compatibility_errors(&graph).is_empty());
}
