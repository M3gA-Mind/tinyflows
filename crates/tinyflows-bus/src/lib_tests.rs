use super::*;

#[test]
fn interfaces_share_one_bus_name_with_distinct_paths() {
    let paths = [
        OBJECT_PATH,
        runs::OBJECT_PATH,
        catalog::OBJECT_PATH,
        schedule::OBJECT_PATH,
    ];
    for (i, a) in paths.iter().enumerate() {
        for b in &paths[i + 1..] {
            assert_ne!(a, b);
        }
    }
    assert!(runs::INTERFACE.starts_with("ai.tinyhumans.tinyflows."));
    assert!(catalog::INTERFACE.starts_with("ai.tinyhumans.tinyflows."));
}

#[test]
fn method_constants_match_advertised_members() {
    assert_eq!(
        runs::METHODS,
        &[
            runs::methods::START_RUN,
            runs::methods::RESUME_RUN,
            runs::methods::CANCEL_RUN,
            runs::methods::POLL_RUN
        ]
    );
}

#[test]
fn authoring_contract_version_and_methods_pin_the_new_requests() {
    assert_eq!(CONTRACT_VERSION, 2);
    assert_eq!(
        METHODS,
        [
            "ValidateGraph",
            "BindingFailures",
            "AuthoringFailures",
            "UnresolvableToolArgs",
            "BuilderFailures",
            "ImportGraph",
        ]
    );
    assert_eq!(methods::VALIDATE_GRAPH, "ValidateGraph");
    assert_eq!(methods::IMPORT_GRAPH, "ImportGraph");
    let request = ValidateGraphRequest {
        graph: serde_json::json!({"nodes":[],"edges":[]}),
        host: None,
    };
    assert_eq!(
        serde_json::to_value(&request).unwrap(),
        serde_json::json!({"graph":{"nodes":[],"edges":[]},"host":null})
    );
    let decoded: ValidateGraphRequest =
        serde_json::from_value(serde_json::to_value(request).unwrap()).unwrap();
    assert_eq!(decoded.host, None);
    let import = ImportGraphRequest {
        graph: serde_json::json!({"nodes":[],"edges":[]}),
        format: Some("auto".into()),
    };
    assert_eq!(
        serde_json::to_value(&import).unwrap(),
        serde_json::json!({"graph":{"nodes":[],"edges":[]},"format":"auto"})
    );
    let imported: ImportGraphRequest =
        serde_json::from_value(serde_json::to_value(import).unwrap()).unwrap();
    assert_eq!(imported.format.as_deref(), Some("auto"));
    wire_import_result_round_trip();
}

fn wire_import_result_round_trip() {
    let value = ImportGraphResult {
        graph: Some(serde_json::json!({"nodes":[],"edges":[]})),
        warnings: vec!["one approximation".into()],
        errors: vec![ValidationError {
            code: "missing_trigger".into(),
            message: "missing trigger".into(),
            node_id: None,
            field: None,
        }],
    };
    assert_eq!(
        serde_json::to_value(&value).unwrap(),
        serde_json::json!({
            "graph":{"nodes":[],"edges":[]},
            "warnings":["one approximation"],
            "errors":[{"code":"missing_trigger","message":"missing trigger","node_id":null,"field":null}]
        })
    );
    let decoded: ImportGraphResult =
        serde_json::from_value(serde_json::to_value(&value).unwrap()).unwrap();
    assert_eq!(decoded, value);
}

#[test]
fn run_event_round_trips() {
    let ev = runs::RunEvent {
        seq: 3,
        kind: "step".into(),
        data: serde_json::json!({"a": 1}),
    };
    let back: runs::RunEvent = serde_json::from_str(&serde_json::to_string(&ev).unwrap()).unwrap();
    assert_eq!(ev, back);
}
