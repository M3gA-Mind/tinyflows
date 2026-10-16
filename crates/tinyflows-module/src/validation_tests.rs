use super::*;
use serde_json::json;

fn valid_graph() -> Value {
    json!({"id": "candidate", "nodes": [
        {"id": "start", "kind": "trigger", "name": "Start", "config": {}}
    ], "edges": []})
}

#[tokio::test]
async fn valid_candidate_returns_migrated_graph_without_errors() {
    let report = validate_graph(valid_graph(), None).await.unwrap();
    assert!(report.is_valid(), "{:?}", report.errors);
    let graph = report.graph.unwrap();
    assert_eq!(graph["nodes"][0]["id"], "start");
}

#[tokio::test]
async fn unparseable_candidate_returns_structured_failure_without_graph() {
    let report = validate_graph(json!({"nodes": "invalid"}), None)
        .await
        .unwrap();
    assert!(!report.is_valid());
    assert!(report.graph.is_none());
    assert_eq!(report.errors.len(), 1);
    assert_eq!(report.errors[0].code, "unparseable_graph");
}

#[tokio::test]
async fn structural_failures_are_aggregated_and_preserve_candidate() {
    let mut graph = valid_graph();
    graph["nodes"] = json!([]);
    graph["edges"] = json!([{"from_node": "missing", "to_node": "absent"}]);
    let report = validate_graph(graph, None).await.unwrap();
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

#[tokio::test]
async fn unsupported_engine_topology_is_reported_after_structural_validation() {
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
    let report = validate_graph(json!({"nodes":nodes,"edges":edges}), None)
        .await
        .unwrap();
    assert!(!report.is_valid());
    assert_eq!(report.errors.len(), 1, "{:?}", report.errors);
    assert_eq!(
        report.errors[0].code,
        "unsupported_nested_conditional_fan_in"
    );
    assert_eq!(report.errors[0].node_id.as_deref(), Some("merge"));
}

#[tokio::test]
async fn migrated_candidate_preserves_unknown_editor_metadata() {
    let mut candidate = valid_graph();
    candidate["editor"] = json!({"viewport": {"x": 17}, "selected": "start"});
    candidate["nodes"][0]["position"] = json!({"x": 23, "y": 42});
    let report = validate_graph(candidate.clone(), None).await.unwrap();
    assert!(report.is_valid());
    let migrated = report.graph.unwrap();
    assert_eq!(migrated["editor"], candidate["editor"]);
    assert_eq!(
        migrated["nodes"][0]["position"],
        candidate["nodes"][0]["position"]
    );
    assert_eq!(
        migrated["schema_version"],
        tinyflows::model::CURRENT_SCHEMA_VERSION
    );
}

#[tokio::test]
async fn input_validation_errors_identify_the_affected_field() {
    let mut candidate = valid_graph();
    candidate["inputs"] = json!([
        {"name": "repo-url", "type": "string"},
        {"name": "depth", "type": "number", "default": "3"},
        {"name": "depth", "type": "number"}
    ]);
    let report = validate_graph(candidate, None).await.unwrap();
    for (code, field) in [
        ("invalid_input_name", "repo-url"),
        ("input_default_type_mismatch", "depth"),
        ("duplicate_input_name", "depth"),
    ] {
        let error = report
            .errors
            .iter()
            .find(|error| error.code == code)
            .unwrap();
        assert_eq!(error.field.as_deref(), Some(field), "{code}");
    }
}

#[tokio::test]
async fn future_schema_returns_unparseable_failure_without_a_migrated_graph() {
    let mut candidate = valid_graph();
    candidate["schema_version"] = json!(tinyflows::model::CURRENT_SCHEMA_VERSION + 1);
    let report = validate_graph(candidate, None).await.unwrap();
    assert!(report.graph.is_none());
    assert_eq!(report.errors[0].code, "unparseable_graph");
}
