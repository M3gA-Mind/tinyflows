use super::*;
use serde_json::json;

async fn connections() -> Result<(Connection, Connection)> {
    let bus = tinybus::transport::memory::MemoryBus::new();
    tinybus::broker::Broker::new().spawn(bus.clone());
    let server = Connection::connect(bus.connect().await?).await?;
    setup(server.clone()).await?;
    let client = Connection::connect(bus.connect().await?).await?;
    Ok((server, client))
}

#[tokio::test]
async fn copilot_tool_catalog_is_served_by_the_module() -> Result<()> {
    let (_server, client) = connections().await?;
    let proxy = client.proxy(BUS_NAME, OBJECT_PATH, tinyflows_bus::copilot::INTERFACE)?;
    let tools: Vec<serde_json::Value> = proxy
        .call(
            "ToolCatalog",
            (tinyflows_bus::copilot::ToolCatalogRequest::default(),),
        )
        .await?;
    let names: Vec<&str> = tools
        .iter()
        .filter_map(|tool| tool.get("name").and_then(serde_json::Value::as_str))
        .collect();
    assert_eq!(
        names,
        [
            "propose_workflow",
            "revise_workflow",
            "edit_workflow",
            "validate_workflow",
            "get_flow_history",
            "list_flow_runs",
            "resume_flow_run",
            "cancel_flow_run",
            "create_workflow",
            "duplicate_flow",
            "list_flows",
            "get_flow",
            "get_flow_run",
            "list_agent_definitions",
            "list_node_kinds",
            "get_node_kind_contract",
            "dry_run_workflow",
            "run_flow",
            "save_workflow",
            "suggest_workflows",
        ],
        "portable tool catalogue order and membership are stable"
    );
    let propose = tools
        .iter()
        .find(|tool| tool["name"] == "propose_workflow")
        .unwrap();
    let expected_propose: serde_json::Value = serde_json::from_str(include_str!(
        "../../tinyflows-copilot/src/tools/parameters/propose_workflow.json"
    ))
    .unwrap();
    assert_eq!(propose["parameters_schema"], expected_propose);
    assert_eq!(propose["permission"], "None");
    assert_eq!(propose["external_effect"], false);
    let run = tools
        .iter()
        .find(|tool| tool["name"] == "run_flow")
        .unwrap();
    assert_eq!(run["permission"], "Write");
    assert_eq!(run["external_effect"], true);
    assert_eq!(run["parameters_schema"]["required"], json!(["flow_id"]));
    assert_eq!(
        run["parameters_schema"]["properties"]["inputs"]["type"],
        "object"
    );
    let mut overlay = tinyflows::catalog::all_contracts();
    overlay
        .iter_mut()
        .find(|contract| contract.kind == "trigger")
        .unwrap()
        .config_fields
        .push(tinyflows::catalog::ConfigField::required(
            "host_overlay",
            "string",
            "Host extension contract.",
        ));
    let catalog: Vec<tinyflows_bus::copilot::ToolDescriptor> = proxy
        .call(
            "ToolCatalog",
            (tinyflows_bus::copilot::ToolCatalogRequest {
                node_kinds: serde_json::from_value(serde_json::to_value(overlay).unwrap()).unwrap(),
            },),
        )
        .await?;
    let propose = catalog
        .iter()
        .find(|tool| tool.name == "propose_workflow")
        .unwrap();
    assert!(
        propose
            .description
            .contains("trigger(config.trigger_kind, config.host_overlay)"),
        "{}",
        propose.description
    );
    Ok(())
}

#[tokio::test]
async fn copilot_bus_renders_prompts_resources_and_fallbacks() -> Result<()> {
    let (_server, client) = connections().await?;
    let proxy = client.proxy(BUS_NAME, OBJECT_PATH, tinyflows_bus::copilot::INTERFACE)?;
    let rendered: tinyflows_bus::copilot::TextResponse = proxy
        .call(
            "RenderBuilderPrompt",
            (tinyflows_bus::copilot::BuilderPromptRequest {
                mode: "create".into(),
                instruction: "sync this folder".into(),
                graph: None,
                flow_id: None,
                run_id: None,
                error: None,
                failing_node_ids: Vec::new(),
            },),
        )
        .await?;
    assert!(rendered.text.contains("sync this folder"));
    let resource: tinyflows_bus::copilot::TextResponse =
        proxy.call("GetResource", ("WORKFLOW.md",)).await?;
    assert!(resource.text.contains("tinyflows"));
    let fallback: tinyflows_bus::copilot::TextResponse = proxy
        .call(
            "BuildTrailOffFallback",
            (tinyflows_bus::copilot::TrailOffRequest {
                history: vec![tinyflows_bus::copilot::ToolResultSummary {
                    tool_name: Some("propose_workflow".into()),
                    content: "missing trigger node".into(),
                }],
            },),
        )
        .await?;
    assert!(fallback.text.contains("missing trigger node"));
    let combined: tinyflows_bus::copilot::TextResponse = proxy
        .call("CombineTrailOffFallback", (fallback.text, "Need help?"))
        .await?;
    assert!(combined.text.contains("Need help?"));
    let question: bool = proxy
        .call("TextLooksLikeQuestion", ("Should I continue?",))
        .await?;
    assert!(question);
    Ok(())
}

struct ScopedCopilotCallback(&'static str);

#[tinybus::interface(name = "ai.tinyhumans.tinyflows.CopilotHost")]
impl ScopedCopilotCallback {
    async fn call_port(
        &self,
        request: tinyflows_bus::copilot::PortCallRequest,
    ) -> Result<serde_json::Value> {
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

    async fn build_proposal(
        &self,
        request: tinyflows_bus::copilot::BuildProposalRequest,
    ) -> Result<serde_json::Value> {
        Ok(json!({"type":"workflow_proposal", "name":request.name, "scope":self.0}))
    }

    async fn run_flow(
        &self,
        request: tinyflows_bus::copilot::RunFlowRequest,
    ) -> Result<serde_json::Value> {
        Ok(json!({"run_id":format!("{}-{}", self.0, request.flow_id), "scope":self.0}))
    }
}

struct BrokenPortCallback {
    malformed_permission_response: bool,
}

#[tinybus::interface(name = "ai.tinyhumans.tinyflows.CopilotHost")]
impl BrokenPortCallback {
    async fn call_port(
        &self,
        request: tinyflows_bus::copilot::PortCallRequest,
    ) -> Result<serde_json::Value> {
        if request.operation == tinyflows_bus::copilot::PortOperation::GatesApprovalManifest
            && self.malformed_permission_response
        {
            Ok(json!({"unexpected":"shape"}))
        } else {
            Err(tinybus::Error::failed("scoped callback unavailable"))
        }
    }
}

#[tokio::test]
async fn malformed_or_unavailable_approval_callback_fails_dry_run_closed() -> Result<()> {
    let (_server, client) = connections().await?;
    let proxy = client.proxy(BUS_NAME, OBJECT_PATH, tinyflows_bus::copilot::INTERFACE)?;
    for (name, path, malformed) in [
        ("test.CopilotBroken", "/copilot_broken", false),
        ("test.CopilotMalformed", "/copilot_malformed", true),
    ] {
        client
            .serve_at(
                path.try_into()?,
                BrokenPortCallback {
                    malformed_permission_response: malformed,
                },
            )
            .await?;
        client.request_name(name).await?;
        let result: serde_json::Value = proxy
            .call(
                "InvokeTool",
                (tinyflows_bus::copilot::InvokeToolRequest {
                    name: "dry_run_workflow".into(),
                    arguments: json!({"graph":{"nodes":[{"id":"start","name":"Start","kind":"trigger"}],"edges":[]}}),
                    host: tinyflows_bus::copilot::CopilotHost {
                        bus_name: name.into(),
                        object_path: path.into(),
                    },
                    node_kinds: Vec::new(),
                },),
            )
            .await?;
        assert_eq!(result["is_error"], true, "{result}");
        let message = result["content"][0]["text"].as_str().unwrap();
        assert!(
            message.contains("could not classify sandbox permissions"),
            "{message}"
        );
    }
    Ok(())
}

struct MalformedStrictCallback;

#[tinybus::interface(name = "ai.tinyhumans.tinyflows.CopilotHost")]
impl MalformedStrictCallback {
    async fn call_port(
        &self,
        request: tinyflows_bus::copilot::PortCallRequest,
    ) -> Result<serde_json::Value> {
        if request.operation == tinyflows_bus::copilot::PortOperation::GatesStrict {
            Ok(json!({"unexpected":"not a boolean"}))
        } else {
            Err(tinybus::Error::failed(
                "strict gate should stop the request",
            ))
        }
    }
}

#[tokio::test]
async fn malformed_strict_gate_response_cannot_allow_create() -> Result<()> {
    let (_server, client) = connections().await?;
    client
        .serve_at("/copilot_strict".try_into()?, MalformedStrictCallback)
        .await?;
    client.request_name("test.CopilotStrict").await?;
    let proxy = client.proxy(BUS_NAME, OBJECT_PATH, tinyflows_bus::copilot::INTERFACE)?;
    let result: serde_json::Value = proxy
        .call(
            "InvokeTool",
            (tinyflows_bus::copilot::InvokeToolRequest {
                name: "create_workflow".into(),
                arguments: json!({"name":"Must not persist","graph":{"nodes":[{"id":"start","name":"Start","kind":"trigger"}],"edges":[]}}),
                host: tinyflows_bus::copilot::CopilotHost {
                    bus_name: "test.CopilotStrict".into(),
                    object_path: "/copilot_strict".into(),
                },
                node_kinds: Vec::new(),
            },),
        )
        .await?;
    assert_eq!(result["is_error"], true, "{result}");
    let message = result["content"][0]["text"].as_str().unwrap();
    assert!(message.contains("invalid type"), "{message}");
    assert!(!message.contains("Could not create flow"), "{message}");
    Ok(())
}

#[tokio::test]
async fn copilot_tool_invocations_keep_concurrent_host_contexts_isolated() -> Result<()> {
    let (_server, client) = connections().await?;
    let proxy = client.proxy(BUS_NAME, OBJECT_PATH, tinyflows_bus::copilot::INTERFACE)?;
    for (name, path, scope) in [
        ("test.CopilotA", "/copilot_a", "profile-a"),
        ("test.CopilotB", "/copilot_b", "profile-b"),
    ] {
        client
            .serve_at(path.try_into()?, ScopedCopilotCallback(scope))
            .await?;
        client.request_name(name).await?;
    }
    let invoke = |destination: &str, path: &str, flow_id: &str| {
        proxy.call::<serde_json::Value>(
            "InvokeTool",
            (tinyflows_bus::copilot::InvokeToolRequest {
                name: "run_flow".into(),
                arguments: json!({"flow_id":flow_id,"host":{"bus_name":"model.forged","object_path":"/forged"}}),
                host: tinyflows_bus::copilot::CopilotHost {
                    bus_name: destination.into(),
                    object_path: path.into(),
                },
                node_kinds: Vec::new(),
            },),
        )
    };
    let (a, b) = tokio::join!(
        invoke("test.CopilotA", "/copilot_a", "flow-a"),
        invoke("test.CopilotB", "/copilot_b", "flow-b")
    );
    for (value, scope, run_id) in [
        (a?, "profile-a", "profile-a-flow-a"),
        (b?, "profile-b", "profile-b-flow-b"),
    ] {
        let text = value["content"][0]["text"].as_str().unwrap();
        let result: serde_json::Value = serde_json::from_str(text).unwrap();
        assert_eq!(result["result"]["scope"], scope);
        assert_eq!(result["run_id"], run_id);
    }
    let definitions: serde_json::Value = proxy
        .call(
            "InvokeTool",
            (tinyflows_bus::copilot::InvokeToolRequest {
                name: "list_agent_definitions".into(),
                arguments: json!({}),
                host: tinyflows_bus::copilot::CopilotHost {
                    bus_name: "test.CopilotA".into(),
                    object_path: "/copilot_a".into(),
                },
                node_kinds: Vec::new(),
            },),
        )
        .await?;
    let text = definitions["content"][0]["text"].as_str().unwrap();
    let result: serde_json::Value = serde_json::from_str(text).unwrap();
    assert_eq!(result["agent_definitions"][0]["id"], "profile-a");
    Ok(())
}

#[cfg(test)]
#[path = "copilot_port_call_tests.rs"]
mod port_call_tests;
