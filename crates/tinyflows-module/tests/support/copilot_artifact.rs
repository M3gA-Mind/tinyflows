use serde_json::{Value, json};
use tinybus::{Connection, Proxy};

#[path = "copilot_port_artifact.rs"]
mod copilot_port_artifact;

struct ScopedCallback(&'static str);

#[tinybus::interface(name = "ai.tinyhumans.tinyflows.CopilotHost")]
impl ScopedCallback {
    async fn call_port(
        &self,
        request: tinyflows_bus::copilot::PortCallRequest,
    ) -> tinybus::Result<Value> {
        if request.operation == tinyflows_bus::copilot::PortOperation::AgentsList {
            Ok(
                json!([{"id":self.0,"name":self.0,"description":"scoped","model":null,"tools":[],"tags":[]}]),
            )
        } else {
            Err(tinybus::Error::failed(format!(
                "unexpected port operation {:?}",
                request.operation
            )))
        }
    }

    async fn run_flow(
        &self,
        request: tinyflows_bus::copilot::RunFlowRequest,
    ) -> tinybus::Result<Value> {
        Ok(json!({"run_id":format!("{}-{}", self.0, request.flow_id), "scope":self.0}))
    }

    async fn build_proposal(
        &self,
        request: tinyflows_bus::copilot::BuildProposalRequest,
    ) -> tinybus::Result<Value> {
        Ok(json!({"type":"workflow_proposal", "name":request.name, "scope":self.0}))
    }
}

pub async fn verify_surface(connection: &Connection) {
    let proxy: Proxy = connection
        .proxy(
            tinyflows_bus::BUS_NAME,
            tinyflows_bus::OBJECT_PATH,
            tinyflows_bus::copilot::INTERFACE,
        )
        .unwrap();
    let catalog: Vec<tinyflows_bus::copilot::ToolDescriptor> = proxy
        .call(
            "ToolCatalog",
            (tinyflows_bus::copilot::ToolCatalogRequest::default(),),
        )
        .await
        .unwrap();
    let propose = catalog
        .iter()
        .find(|tool| tool.name == "propose_workflow")
        .unwrap();
    assert_eq!(propose.permission, "None");
    assert!(!propose.external_effect);
    assert!(
        propose.parameters_schema["required"]
            .as_array()
            .unwrap()
            .contains(&"name".into())
    );
    let run = catalog.iter().find(|tool| tool.name == "run_flow").unwrap();
    assert_eq!(run.permission, "Write");
    assert!(run.external_effect);
    assert_eq!(
        catalog.len(),
        20,
        "the full portable copilot belt is served"
    );

    let prompt: tinyflows_bus::copilot::TextResponse = proxy
        .call(
            "RenderBuilderPrompt",
            (tinyflows_bus::copilot::BuilderPromptRequest {
                mode: "create".into(),
                instruction: "sync a folder".into(),
                graph: None,
                flow_id: None,
                run_id: None,
                error: None,
                failing_node_ids: vec![],
            },),
        )
        .await
        .unwrap();
    assert!(prompt.text.contains("sync a folder"));

    let resource: tinyflows_bus::copilot::TextResponse =
        proxy.call("GetResource", ("WORKFLOW.md",)).await.unwrap();
    assert!(resource.text.contains("tinyflows"));
    let resources: Vec<tinyflows_bus::copilot::ResourceFile> =
        proxy.call("ListResources", ()).await.unwrap();
    let paths: Vec<&str> = resources.iter().map(|file| file.path.as_str()).collect();
    assert_eq!(
        paths,
        [
            "WORKFLOW.md",
            "references/expressions.md",
            "references/node-config.md",
            "references/dry-run.md",
        ]
    );
    assert!(resources.iter().all(|file| !file.contents.is_empty()));
    let standing_prompt: tinyflows_bus::copilot::TextResponse = proxy
        .call("GetPrompt", ("workflow_builder",))
        .await
        .unwrap();
    assert!(standing_prompt.text.contains("# Workflow Builder"));
    let failed: bool = proxy
        .call("IsBackendOrInfrastructureFailure", ("HTTP 503",))
        .await
        .unwrap();
    assert!(failed);
    let repair: Option<String> = proxy
        .call("BackendRepairMessage", ("connection refused",))
        .await
        .unwrap();
    assert!(repair.unwrap().contains("external service"));

    connection
        .serve_at(
            "/copilot_scope_a".try_into().unwrap(),
            ScopedCallback("scope-a"),
        )
        .await
        .unwrap();
    connection.request_name("test.CopilotScopeA").await.unwrap();
    connection
        .serve_at(
            "/copilot_scope_b".try_into().unwrap(),
            ScopedCallback("scope-b"),
        )
        .await
        .unwrap();
    connection.request_name("test.CopilotScopeB").await.unwrap();
    let invoke = |bus_name: &str, object_path: &str, flow_id: &str| {
        proxy.call::<Value>(
            "InvokeTool",
            (tinyflows_bus::copilot::InvokeToolRequest {
                name: "run_flow".into(),
                arguments: json!({"flow_id":flow_id,"host":{"bus_name":"forged","object_path":"/from-model"}}),
                host: tinyflows_bus::copilot::CopilotHost {
                    bus_name: bus_name.into(),
                    object_path: object_path.into(),
                },
                node_kinds: vec![],
            },),
        )
    };
    let (a, b) = tokio::join!(
        invoke("test.CopilotScopeA", "/copilot_scope_a", "flow-a"),
        invoke("test.CopilotScopeB", "/copilot_scope_b", "flow-b")
    );
    for (value, scope, run_id) in [
        (a.unwrap(), "scope-a", "scope-a-flow-a"),
        (b.unwrap(), "scope-b", "scope-b-flow-b"),
    ] {
        let result: Value =
            serde_json::from_str(value["content"][0]["text"].as_str().unwrap()).unwrap();
        assert_eq!(result["result"]["scope"], scope);
        assert_eq!(result["run_id"], run_id);
    }
    let definitions: Value = proxy
        .call(
            "InvokeTool",
            (tinyflows_bus::copilot::InvokeToolRequest {
                name: "list_agent_definitions".into(),
                arguments: json!({}),
                host: tinyflows_bus::copilot::CopilotHost {
                    bus_name: "test.CopilotScopeA".into(),
                    object_path: "/copilot_scope_a".into(),
                },
                node_kinds: vec![],
            },),
        )
        .await
        .unwrap();
    let text = definitions["content"][0]["text"].as_str().unwrap();
    let result: Value = serde_json::from_str(text).unwrap();
    assert_eq!(result["agent_definitions"][0]["id"], "scope-a");

    copilot_port_artifact::verify_port_calls(connection, &proxy).await;
}
