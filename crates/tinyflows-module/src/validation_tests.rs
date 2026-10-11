use super::*;
use serde_json::json;

fn valid_graph() -> Value {
    json!({"id": "candidate", "nodes": [
        {"id": "start", "kind": "trigger", "name": "Start", "config": {}}
    ], "edges": []})
}

#[test]
fn valid_candidate_returns_migrated_graph_without_errors() {
    let report = validate_graph(valid_graph()).unwrap();
    assert!(report.is_valid(), "{:?}", report.errors);
    let graph = report.graph.unwrap();
    assert_eq!(graph["nodes"][0]["id"], "start");
}

#[test]
fn unparseable_candidate_returns_structured_failure_without_graph() {
    let report = validate_graph(json!({"nodes": "invalid"})).unwrap();
    assert!(!report.is_valid());
    assert!(report.graph.is_none());
    assert_eq!(report.errors.len(), 1);
    assert_eq!(report.errors[0].code, "unparseable_graph");
}

#[test]
fn structural_failures_are_aggregated_and_preserve_candidate() {
    let mut graph = valid_graph();
    graph["nodes"] = json!([]);
    graph["edges"] = json!([{"from_node": "missing", "to_node": "absent"}]);
    let report = validate_graph(graph).unwrap();
    assert!(!report.is_valid());
    assert!(report.graph.is_some());
    assert!(report.errors.len() > 1, "{:?}", report.errors);
    assert!(
        report
            .errors
            .iter()
            .any(|error| error.code == "missing_trigger")
    );
    assert!(
        report
            .errors
            .iter()
            .any(|error| error.code == "unknown_node")
    );
}

#[test]
fn unsupported_engine_topology_is_reported_after_structural_validation() {
    let nodes = [
        ("start", "trigger", json!({})),
        ("outer", "condition", json!({"field":"o"})),
        ("inner", "condition", json!({"field":"i"})),
        ("a", "output_parser", json!({})),
        ("c", "output_parser", json!({})),
        ("outer_else", "output_parser", json!({})),
        ("inner_else", "output_parser", json!({})),
        ("merge", "merge", json!({})),
    ]
    .map(|(id, kind, config)| json!({"id":id,"name":id,"kind":kind,"config":config}));
    let edges = [
        ("start", "main", "outer"),
        ("start", "main", "c"),
        ("outer", "true", "inner"),
        ("outer", "false", "outer_else"),
        ("inner", "true", "a"),
        ("inner", "false", "inner_else"),
        ("a", "main", "merge"),
        ("c", "main", "merge"),
    ]
    .map(|(from, port, to)| json!({"from_node":from,"from_port":port,"to_node":to}));
    let report = validate_graph(json!({"nodes":nodes,"edges":edges})).unwrap();
    assert!(!report.is_valid());
    assert_eq!(report.errors.len(), 1, "{:?}", report.errors);
    assert_eq!(
        report.errors[0].code,
        "unsupported_nested_conditional_fan_in"
    );
    assert_eq!(report.errors[0].node_id.as_deref(), Some("merge"));
}
