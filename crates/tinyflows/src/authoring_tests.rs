use super::*;
use serde_json::json;

#[test]
fn accepts_a_valid_graph() {
    let graph = validate_and_migrate_graph(json!({
        "name": "ok",
        "nodes": [{ "id": "t", "kind": "trigger", "name": "Trigger" }],
        "edges": []
    }))
    .expect("valid");
    assert_eq!(graph.nodes.len(), 1);
}

#[test]
fn rejects_a_graph_without_a_trigger() {
    assert!(validate_and_migrate_graph(json!({"name": "x", "nodes": [], "edges": []})).is_err());
}

#[test]
fn names_the_member_that_failed_to_deserialize() {
    let err = validate_and_migrate_graph(json!({"name": "x", "nodes": [{"id": 1}], "edges": []}))
        .unwrap_err();
    assert!(err.contains("nodes[0]"), "{err}");
}
