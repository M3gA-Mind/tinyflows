use super::*;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};

fn round_trip<T>(value: T)
where
    T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug,
{
    let encoded = serde_json::to_vec(&value).unwrap();
    assert_eq!(serde_json::from_slice::<T>(&encoded).unwrap(), value);
}

fn storage() -> CatalogStorage {
    CatalogStorage {
        callback: CallbackAddress {
            destination: "test.CatalogStorage".into(),
            object_path: "/agent/alice/CatalogStorage".into(),
        },
    }
}

fn workflow() -> Workflow {
    Workflow {
        id: "flow-1".into(),
        name: "Flow".into(),
        enabled: false,
        graph: json!({"nodes": [], "edges": []}),
        created_at: "created".into(),
        updated_at: "updated".into(),
        last_run_at: None,
        last_status: None,
        require_approval: false,
    }
}

fn draft() -> FlowDraft {
    FlowDraft {
        id: "draft-1".into(),
        flow_id: None,
        name: "Draft".into(),
        graph: json!({"nodes": []}),
        origin: DraftOrigin::Import,
        created_at: "created".into(),
        updated_at: "updated".into(),
    }
}

fn suggestion() -> FlowSuggestion {
    FlowSuggestion {
        id: "suggestion-1".into(),
        title: "Title".into(),
        one_liner: "One line".into(),
        rationale: "Reason".into(),
        trigger_hint: Some("manual".into()),
        steps_outline: vec!["step".into()],
        suggested_connections: vec!["http_cred:billing".into()],
        suggested_slugs: vec!["stripe:create".into()],
        build_prompt: "build".into(),
        confidence: 0.75,
        status: SuggestionStatus::Dismissed,
        created_at: "created".into(),
        source_run_id: Some("run-1".into()),
    }
}

fn run() -> FlowRun {
    FlowRun {
        id: "run-1".into(),
        flow_id: "flow-1".into(),
        thread_id: "thread-1".into(),
        status: "pending_approval".into(),
        started_at: "started".into(),
        finished_at: None,
        steps: vec![FlowRunStep {
            node_id: "node-1".into(),
            output: json!({"ok":true}),
            port: Some("main".into()),
            status: Some("success".into()),
            duration_ms: Some(12),
            diagnostics: vec![json!({"location":"args.to"})],
        }],
        pending_approvals: vec!["node-2".into()],
        error: None,
        graph_hash: Some("hash".into()),
    }
}

#[test]
fn every_extended_catalog_type_round_trips() {
    let scope = storage();
    let draft = draft();
    let flow = workflow();
    let suggestion = suggestion();
    let run = run();
    round_trip(DraftOrigin::Canvas);
    round_trip(draft.clone());
    round_trip(CreateDraftRequest {
        storage: scope.clone(), flow_id: Some("flow-1".into()), name: "Draft".into(),
        graph: json!({"incomplete": true}), origin: DraftOrigin::Chat,
    });
    round_trip(DraftIdRequest { storage: scope.clone(), id: draft.id.clone() });
    round_trip(UpdateDraftRequest {
        storage: scope.clone(), id: draft.id.clone(), name: Some("New".into()),
        graph: Some(json!({"nodes": []})), flow_id: Some(None),
    });
    round_trip(PromoteDraftRequest {
        storage: scope.clone(),
        host: CallbackAddress { destination: "test.Promotion".into(), object_path: "/alice/Promotion".into() },
        id: draft.id.clone(), require_approval: None,
    });
    round_trip(DraftPromotionOperation::Create {
        name: "New flow".into(), graph: json!({"nodes": []}), require_approval: false,
    });
    round_trip(DraftPromotionOperation::Update {
        id: "flow-1".into(), name: "Changed".into(), graph: json!({}),
        require_approval: None, expected_version: None,
    });
    round_trip(SavePromotedDraftRequest { operation: DraftPromotionOperation::Create {
        name: "Flow".into(), graph: json!({}), require_approval: true,
    }});
    round_trip(SavePromotedDraftResult { workflow: flow.clone(), metadata: json!({"logs": ["saved"]}) });
    round_trip(DraftPromotionFailure { code: "invalid_graph".into(), message: "invalid".into(), details: Some(json!({"node":"n1"})) });
    round_trip(SavePromotedDraftResponse::Error(DraftPromotionFailure {
        code: "conflict".into(), message: "changed".into(), details: None,
    }));
    round_trip(PromoteDraftResult {
        workflow: flow.clone(), metadata: json!({"events": ["FlowChanged"]}),
        draft_deleted: false, cleanup_warning: Some("delete failed".into()),
    });
    round_trip(PromoteDraftResponse::Ok(PromoteDraftResult {
        workflow: flow.clone(), metadata: Value::Null, draft_deleted: true, cleanup_warning: None,
    }));
    round_trip(SuggestionStatus::Built);
    round_trip(suggestion.clone());
    round_trip(UpsertSuggestionsRequest { storage: scope.clone(), suggestions: vec![suggestion.clone()] });
    round_trip(ListSuggestionsRequest { storage: scope.clone(), status: Some(SuggestionStatus::New), limit: 3 });
    round_trip(SetSuggestionStatusRequest { storage: scope.clone(), id: suggestion.id, status: SuggestionStatus::Built });
    round_trip(run.clone());
    round_trip(run.steps[0].clone());
    round_trip(RunIdRequest { storage: scope.clone(), id: run.id.clone() });
    round_trip(ListRunsRequest { storage: scope.clone(), flow_id: run.flow_id.clone(), limit: 10 });
    round_trip(ListAllRunsRequest { storage: scope.clone(), limit: 10 });
    round_trip(PruneRunsRequest { storage: scope, flow_id: run.flow_id, keep: 5 });
}

#[test]
fn extended_catalog_wire_names_and_optional_patch_states_are_pinned() {
    let scope = storage();
    assert_eq!(serde_json::to_value(DraftOrigin::Import).unwrap(), json!("import"));
    assert_eq!(serde_json::to_value(draft()).unwrap(), json!({
        "id":"draft-1", "name":"Draft", "graph":{"nodes":[]}, "origin":"import",
        "created_at":"created", "updated_at":"updated"
    }));
    assert_eq!(serde_json::to_value(UpdateDraftRequest {
        storage: scope.clone(), id: "draft-1".into(), name: None, graph: None, flow_id: None,
    }).unwrap(), json!({"storage":scope,"id":"draft-1"}));
    let cleared: UpdateDraftRequest = serde_json::from_value(json!({
        "storage":storage(), "id":"draft-1", "flow_id":null
    })).unwrap();
    assert_eq!(cleared.flow_id, Some(None));
    let assigned: UpdateDraftRequest = serde_json::from_value(json!({
        "storage":storage(), "id":"draft-1", "flow_id":"flow-2"
    })).unwrap();
    assert_eq!(assigned.flow_id, Some(Some("flow-2".into())));
    let missing: UpdateDraftRequest = serde_json::from_value(json!({"storage":storage(), "id":"draft-1"})).unwrap();
    assert_eq!(missing.flow_id, None);
    assert_eq!(serde_json::to_value(DraftPromotionOperation::Update {
        id: "flow-1".into(), name: "Flow".into(), graph: json!({}),
        require_approval: None, expected_version: None,
    }).unwrap(), json!({"operation":"update", "id":"flow-1", "name":"Flow", "graph":{}, "require_approval":null, "expected_version":null}));
    assert_eq!(serde_json::to_value(SavePromotedDraftResponse::Error(DraftPromotionFailure {
        code: "denied".into(), message: "blocked".into(), details: Some(json!({"reason":"policy"})),
    })).unwrap(), json!({"status":"error", "value":{"code":"denied", "message":"blocked", "details":{"reason":"policy"}}}));
    assert_eq!(serde_json::to_value(SuggestionStatus::Dismissed).unwrap(), json!("dismissed"));
    assert_eq!(serde_json::to_value(FlowRunStep::default()).unwrap(), json!({"node_id":"", "output":null}));
    assert_eq!(
        serde_json::to_value(PruneRunsRequest { storage: storage(), flow_id: "flow-1".into(), keep: 2 }).unwrap(),
        json!({"storage":{"callback":{"destination":"test.CatalogStorage", "object_path":"/agent/alice/CatalogStorage"}}, "flow_id":"flow-1", "keep":2})
    );
}
