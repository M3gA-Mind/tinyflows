use super::*;
use serde_json::Value;
use serde_json::json;

#[test]
fn copilot_request_and_response_dtos_round_trip() {
    let request = BuilderPromptRequest {
        mode: "repair".into(),
        instruction: "fix the branch".into(),
        graph: Some(json!({"nodes": [], "edges": []})),
        flow_id: Some("flow-1".into()),
        run_id: Some("run-2".into()),
        error: Some("external error".into()),
        failing_node_ids: vec!["node-3".into()],
    };
    let round_trip: BuilderPromptRequest =
        serde_json::from_value(serde_json::to_value(&request).unwrap()).unwrap();
    assert_eq!(round_trip, request);

    let trail = TrailOffRequest {
        history: vec![ToolResultSummary {
            tool_name: Some("propose_workflow".into()),
            content: "blocked".into(),
        }],
    };
    assert_eq!(
        serde_json::from_value::<TrailOffRequest>(serde_json::to_value(&trail).unwrap()).unwrap(),
        trail
    );
    let response = TextResponse {
        text: "prompt".into(),
    };
    assert_eq!(
        serde_json::from_value::<TextResponse>(serde_json::to_value(&response).unwrap()).unwrap(),
        response
    );
}

#[test]
fn tool_descriptor_round_trip_keeps_schema_and_safety_metadata() {
    let descriptor = ToolDescriptor {
        name: "run_flow".into(),
        description: "real run".into(),
        parameters_schema: json!({"required": ["flow_id"]}),
        permission: "Write".into(),
        external_effect: true,
    };
    let round_trip: ToolDescriptor =
        serde_json::from_value(serde_json::to_value(&descriptor).unwrap()).unwrap();
    assert_eq!(round_trip, descriptor);
}

#[test]
fn resource_file_wire_shape_is_stable() {
    let file = ResourceFile {
        path: "references/dry-run.md".into(),
        contents: "manual".into(),
    };
    assert_eq!(
        serde_json::to_value(&file).unwrap(),
        json!({"path":"references/dry-run.md","contents":"manual"})
    );
    assert_eq!(
        serde_json::from_value::<ResourceFile>(serde_json::to_value(file).unwrap()).unwrap(),
        ResourceFile {
            path: "references/dry-run.md".into(),
            contents: "manual".into()
        }
    );
}

#[test]
fn host_response_dtos_have_fixed_wire_shapes() {
    let validation = FlowValidation {
        valid: false,
        errors: vec!["missing trigger".into()],
        error_details: vec![FlowValidationError {
            code: "missing_trigger".into(),
            message: "missing trigger".into(),
            node_id: None,
            field: None,
        }],
        warnings: vec![],
    };
    assert_eq!(
        serde_json::to_value(validation).unwrap(),
        json!({"valid":false,"errors":["missing trigger"],"error_details":[{"code":"missing_trigger","message":"missing trigger"}],"warnings":[]})
    );
    let agent = AgentDefinition {
        id: "agent-1".into(),
        name: "Agent".into(),
        description: "Scoped agent".into(),
        model: json!("model-x"),
        tools: json!(["read"]),
        tags: json!(["host"]),
    };
    assert_eq!(
        serde_json::to_value(agent).unwrap(),
        json!({"id":"agent-1","name":"Agent","description":"Scoped agent","model":"model-x","tools":["read"],"tags":["host"]})
    );
    let written = WrittenWorkflow {
        value: crate::catalog::Workflow {
            id: "flow-1".into(),
            name: "Example".into(),
            enabled: false,
            graph: json!({"nodes":[],"edges":[]}),
            created_at: "t0".into(),
            updated_at: "t1".into(),
            last_run_at: None,
            last_status: None,
            require_approval: true,
        },
        logs: vec!["updated".into()],
    };
    assert_eq!(
        serde_json::to_value(written).unwrap(),
        json!({"value":{"id":"flow-1","name":"Example","enabled":false,"graph":{"nodes":[],"edges":[]},"created_at":"t0","updated_at":"t1","last_run_at":null,"last_status":null,"require_approval":true},"logs":["updated"]})
    );
}

#[test]
fn catalog_and_invocation_requests_keep_node_contracts_and_context_address() {
    let catalog = ToolCatalogRequest {
        node_kinds: vec![json!({"kind":"trigger"})],
    };
    assert_eq!(
        serde_json::from_value::<ToolCatalogRequest>(serde_json::to_value(&catalog).unwrap())
            .unwrap(),
        catalog
    );
    let invoke = InvokeToolRequest {
        name: "run_flow".into(),
        arguments: json!({"flow_id":"flow-1"}),
        host: CopilotHost {
            bus_name: "scope.callback".into(),
            object_path: "/per-call/42".into(),
        },
        node_kinds: Vec::new(),
    };
    let wire = serde_json::to_value(&invoke).unwrap();
    assert_eq!(wire["host"]["object_path"], "/per-call/42");
    assert!(wire["arguments"].get("host").is_none());
    assert_eq!(
        serde_json::from_value::<InvokeToolRequest>(wire).unwrap(),
        invoke
    );
}

#[test]
fn every_copilot_callback_dto_round_trips() {
    assert_eq!(HOST_METHODS, ["BuildProposal", "RunFlow", "CallPort"]);
    let build = BuildProposalRequest {
        retry_tool: "propose_workflow".into(),
        name: "Sync".into(),
        graph: json!({"nodes": [], "edges": []}),
        require_approval: true,
        revision: false,
        instruction: None,
        draft_id: None,
        flow_id: None,
    };
    assert_eq!(
        serde_json::from_value::<BuildProposalRequest>(serde_json::to_value(&build).unwrap())
            .unwrap(),
        build
    );
    let run = RunFlowRequest {
        flow_id: "flow-9".into(),
        input: json!({"event":"created"}),
        inputs: serde_json::from_value(json!({"repo":"tinyflows"})).unwrap(),
    };
    assert_eq!(
        serde_json::from_value::<RunFlowRequest>(serde_json::to_value(&run).unwrap()).unwrap(),
        run
    );
    let port = PortCallRequest {
        operation: PortOperation::DraftGet,
        payload: json!({"id":"draft-1"}),
    };
    assert_eq!(
        serde_json::to_value(&port).unwrap(),
        json!({"operation":"draft.get","payload":{"id":"draft-1"}})
    );
    assert_eq!(
        serde_json::from_value::<PortCallRequest>(serde_json::to_value(&port).unwrap()).unwrap(),
        port
    );
    let host = CopilotHost {
        bus_name: "scope.host".into(),
        object_path: "/scope/unique-call".into(),
    };
    assert_eq!(
        serde_json::from_value::<CopilotHost>(serde_json::to_value(&host).unwrap()).unwrap(),
        host
    );
}

#[test]
fn every_port_operation_has_a_pinned_wire_name() {
    let expected = [
        "draft.get",
        "draft.create",
        "draft.update",
        "flows.get",
        "flows.list",
        "flows.load_graph",
        "flows.create",
        "flows.update",
        "flows.set_enabled",
        "flows.duplicate",
        "flows.history",
        "flows.list_runs",
        "flows.get_run",
        "flows.resume",
        "flows.cancel_run",
        "agents.list",
        "suggestions.upsert",
        "gates.validate",
        "gates.run",
        "gates.strict",
        "gates.engine_compatibility",
        "gates.wiring_warnings",
        "gates.approval_manifest",
    ];
    let actual: Vec<String> = port_operations::ALL
        .iter()
        .map(|operation| {
            serde_json::to_value(operation)
                .unwrap()
                .as_str()
                .unwrap()
                .to_owned()
        })
        .collect();
    assert_eq!(actual, expected);
}

#[test]
fn every_port_operation_has_a_fixed_request_fixture() {
    let fixtures = [
        (PortOperation::DraftGet, json!({"id":"draft-1"})),
        (
            PortOperation::DraftCreate,
            json!({"flow_id":null,"name":"A","graph":{},"origin":"chat"}),
        ),
        (
            PortOperation::DraftUpdate,
            json!({"id":"draft-1","name":null,"graph":null}),
        ),
        (PortOperation::FlowsGet, json!({"id":"flow-1"})),
        (PortOperation::FlowsList, Value::Null),
        (PortOperation::FlowsLoadGraph, json!({"id":"flow-1"})),
        (
            PortOperation::FlowsCreate,
            json!({"name":"A","graph":{},"require_approval":true}),
        ),
        (
            PortOperation::FlowsUpdate,
            json!({"id":"flow-1","name":null,"graph":null}),
        ),
        (
            PortOperation::FlowsSetEnabled,
            json!({"id":"flow-1","enabled":false}),
        ),
        (PortOperation::FlowsDuplicate, json!({"id":"flow-1"})),
        (
            PortOperation::FlowsHistory,
            json!({"id":"flow-1","limit":10}),
        ),
        (
            PortOperation::FlowsListRuns,
            json!({"flow_id":"flow-1","limit":10}),
        ),
        (PortOperation::FlowsGetRun, json!({"run_id":"run-1"})),
        (
            PortOperation::FlowsResume,
            json!({"flow_id":"flow-1","run_id":"run-1","approve":[],"reject":[]}),
        ),
        (PortOperation::FlowsCancelRun, json!({"run_id":"run-1"})),
        (PortOperation::AgentsList, Value::Null),
        (PortOperation::SuggestionsUpsert, json!([])),
        (PortOperation::GatesValidate, json!({"nodes":[],"edges":[]})),
        (PortOperation::GatesRun, json!({"nodes":[],"edges":[]})),
        (PortOperation::GatesStrict, json!({"nodes":[],"edges":[]})),
        (
            PortOperation::GatesEngineCompatibility,
            json!({"nodes":[],"edges":[]}),
        ),
        (
            PortOperation::GatesWiringWarnings,
            json!({"nodes":[],"edges":[]}),
        ),
        (
            PortOperation::GatesApprovalManifest,
            json!({"nodes":[],"edges":[]}),
        ),
    ];
    let expected = [
        json!({"operation":"draft.get","payload":{"id":"draft-1"}}),
        json!({"operation":"draft.create","payload":{"flow_id":null,"name":"A","graph":{},"origin":"chat"}}),
        json!({"operation":"draft.update","payload":{"id":"draft-1","name":null,"graph":null}}),
        json!({"operation":"flows.get","payload":{"id":"flow-1"}}),
        json!({"operation":"flows.list","payload":null}),
        json!({"operation":"flows.load_graph","payload":{"id":"flow-1"}}),
        json!({"operation":"flows.create","payload":{"name":"A","graph":{},"require_approval":true}}),
        json!({"operation":"flows.update","payload":{"id":"flow-1","name":null,"graph":null}}),
        json!({"operation":"flows.set_enabled","payload":{"id":"flow-1","enabled":false}}),
        json!({"operation":"flows.duplicate","payload":{"id":"flow-1"}}),
        json!({"operation":"flows.history","payload":{"id":"flow-1","limit":10}}),
        json!({"operation":"flows.list_runs","payload":{"flow_id":"flow-1","limit":10}}),
        json!({"operation":"flows.get_run","payload":{"run_id":"run-1"}}),
        json!({"operation":"flows.resume","payload":{"flow_id":"flow-1","run_id":"run-1","approve":[],"reject":[]}}),
        json!({"operation":"flows.cancel_run","payload":{"run_id":"run-1"}}),
        json!({"operation":"agents.list","payload":null}),
        json!({"operation":"suggestions.upsert","payload":[]}),
        json!({"operation":"gates.validate","payload":{"nodes":[],"edges":[]}}),
        json!({"operation":"gates.run","payload":{"nodes":[],"edges":[]}}),
        json!({"operation":"gates.strict","payload":{"nodes":[],"edges":[]}}),
        json!({"operation":"gates.engine_compatibility","payload":{"nodes":[],"edges":[]}}),
        json!({"operation":"gates.wiring_warnings","payload":{"nodes":[],"edges":[]}}),
        json!({"operation":"gates.approval_manifest","payload":{"nodes":[],"edges":[]}}),
    ];
    let actual: Vec<Value> = fixtures
        .into_iter()
        .map(|(operation, payload)| {
            serde_json::to_value(PortCallRequest { operation, payload }).unwrap()
        })
        .collect();
    assert_eq!(actual, expected);
}
