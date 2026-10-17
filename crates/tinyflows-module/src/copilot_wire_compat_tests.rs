use super::*;
use serde_json::json;

#[test]
fn copilot_callback_response_dtos_deserialize_as_owning_library_types() {
    let validation = tinyflows_bus::copilot::FlowValidation {
        valid: false,
        errors: vec!["missing trigger".into()],
        error_details: vec![tinyflows_bus::copilot::FlowValidationError {
            code: "missing_trigger".into(),
            message: "missing trigger".into(),
            node_id: None,
            field: None,
        }],
        warnings: vec![],
    };
    let library_validation: tinyflows_catalog::types::FlowValidation =
        decode_library_type(validation).unwrap();
    assert!(!library_validation.valid);
    assert_eq!(library_validation.error_details[0].code, "missing_trigger");

    let agent = tinyflows_bus::copilot::AgentDefinition {
        id: "agent-1".into(),
        name: "Agent".into(),
        description: "Scoped agent".into(),
        model: json!("model-x"),
        tools: json!(["read"]),
        tags: json!(["host"]),
    };
    let library_agent: tinyflows_copilot::tools::ports::AgentDefinition =
        decode_library_type(agent).unwrap();
    assert_eq!(library_agent.id, "agent-1");
    assert_eq!(library_agent.tools, json!(["read"]));

    let flow = tinyflows_bus::catalog::Workflow {
        id: "flow-1".into(),
        name: "Example".into(),
        enabled: false,
        graph: json!({"nodes":[{"id":"start","name":"Start","kind":"trigger"}],"edges":[]}),
        created_at: "t0".into(),
        updated_at: "t1".into(),
        last_run_at: None,
        last_status: None,
        require_approval: true,
    };
    let written = tinyflows_bus::copilot::WrittenWorkflow {
        value: flow,
        logs: vec!["updated".into()],
    };
    let library_written: tinyflows_copilot::tools::ports::Written<tinyflows_catalog::types::Flow> =
        decode_library_type(written).unwrap();
    assert_eq!(library_written.value.id, "flow-1");
    assert_eq!(library_written.logs, ["updated"]);
}
