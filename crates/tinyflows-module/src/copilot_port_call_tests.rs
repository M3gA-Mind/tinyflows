use super::*;
use serde_json::json;

struct PortHarness(std::sync::Arc<std::sync::Mutex<Vec<tinyflows_bus::copilot::PortCallRequest>>>);

#[tinybus::interface(name = "ai.tinyhumans.tinyflows.CopilotHost")]
impl PortHarness {
    async fn call_port(
        &self,
        request: tinyflows_bus::copilot::PortCallRequest,
    ) -> Result<serde_json::Value> {
        use tinyflows_bus::copilot::PortOperation as Op;
        self.0.lock().unwrap().push(request.clone());
        let graph = json!({"nodes":[{"id":"start","name":"Start","kind":"trigger","config":{}}],"edges":[]});
        let flow = json!({"id":"flow-1","name":"Example","enabled":false,"graph":graph,"created_at":"t0","updated_at":"t0","last_run_at":null,"last_status":null,"require_approval":true});
        let draft = json!({"id":"draft-1","flow_id":null,"name":"Example","graph":graph,"origin":"chat","created_at":"t0","updated_at":"t0"});
        Ok(match request.operation {
            Op::GatesStrict => json!(true),
            Op::GatesEngineCompatibility | Op::GatesRun | Op::GatesWiringWarnings => json!([]),
            Op::GatesValidate => json!({"valid":true,"errors":[],"warnings":[]}),
            Op::GatesApprovalManifest => json!([]),
            Op::FlowsList => json!([flow]),
            Op::FlowsGet | Op::FlowsCreate | Op::FlowsSetEnabled => flow,
            Op::FlowsUpdate => json!({"value":flow,"logs":[]}),
            Op::FlowsHistory => json!([{
                "id":"revision-1","flow_id":"flow-1","graph":graph,
                "name":"Earlier","require_approval":true,"created_at":"t0"
            }]),
            Op::DraftGet | Op::DraftCreate | Op::DraftUpdate => draft,
            Op::FlowsGetRun => {
                json!({"id":"run-1","flow_id":"flow-1","thread_id":"run-1","status":"running","started_at":"t0","finished_at":null,"error":null})
            }
            Op::FlowsResume => json!({"resumed":true}),
            Op::FlowsCancelRun => json!({"cancelled":true}),
            Op::SuggestionsUpsert => json!(1),
            operation => {
                return Err(tinybus::Error::failed(format!(
                    "unexpected operation {operation:?}"
                )));
            }
        })
    }

    async fn build_proposal(
        &self,
        request: tinyflows_bus::copilot::BuildProposalRequest,
    ) -> Result<serde_json::Value> {
        Ok(json!({"type":"workflow_proposal","name":request.name,"persisted":false}))
    }

    async fn run_flow(
        &self,
        request: tinyflows_bus::copilot::RunFlowRequest,
    ) -> Result<serde_json::Value> {
        Ok(json!({"run_id":format!("run-{}",request.flow_id)}))
    }
}

#[tokio::test]
async fn portable_tool_belt_routes_reads_draft_writes_run_control_and_suggestions() -> Result<()> {
    use tinyflows_bus::copilot::PortOperation as Op;
    let (_server, client) = connections().await?;
    let calls = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    client
        .serve_at(
            "/copilot_port_harness".try_into()?,
            PortHarness(calls.clone()),
        )
        .await?;
    client.request_name("test.CopilotPortHarness").await?;
    let proxy = client.proxy(BUS_NAME, OBJECT_PATH, tinyflows_bus::copilot::INTERFACE)?;
    let host = tinyflows_bus::copilot::CopilotHost {
        bus_name: "test.CopilotPortHarness".into(),
        object_path: "/copilot_port_harness".into(),
    };
    let graph =
        json!({"nodes":[{"id":"start","name":"Start","kind":"trigger","config":{}}],"edges":[]});
    let invoke = |name: &str, arguments: serde_json::Value| {
        proxy.call::<serde_json::Value>(
            "InvokeTool",
            (tinyflows_bus::copilot::InvokeToolRequest {
                name: name.into(),
                arguments,
                host: host.clone(),
                node_kinds: Vec::new(),
            },),
        )
    };
    for (name, args) in [
        ("list_flows", json!({})),
        ("get_flow_history", json!({"flow_id":"flow-1"})),
        (
            "edit_workflow",
            json!({"draft_id":"draft-1","ops":[{"op":"set_node_name","id":"start","name":"Renamed"}]}),
        ),
        ("create_workflow", json!({"name":"New","graph":graph})),
        ("save_workflow", json!({"flow_id":"flow-1","graph":graph})),
        (
            "resume_flow_run",
            json!({"flow_id":"flow-1","run_id":"run-1","approve":["send-node"],"reject":["archive-node"]}),
        ),
        (
            "cancel_flow_run",
            json!({"flow_id":"flow-1","run_id":"run-1"}),
        ),
        (
            "suggest_workflows",
            json!({"suggestions":[{"title":"Daily summary","one_liner":"Summarize updates","rationale":"Repeated task","build_prompt":"Create a daily summary"}]}),
        ),
    ] {
        let result = invoke(name, args).await?;
        assert_eq!(result["is_error"], false, "{name}: {result}");
        if name == "get_flow_history" {
            let text = result["content"][0]["text"].as_str().unwrap();
            let history: serde_json::Value = serde_json::from_str(text).unwrap();
            assert_eq!(history["revisions"][0]["id"], "revision-1");
        }
        if name == "list_flows" {
            let text = result["content"][0]["text"].as_str().unwrap();
            let flows: serde_json::Value = serde_json::from_str(text).unwrap();
            assert_eq!(flows["flows"][0]["id"], "flow-1");
        }
    }
    let calls = calls.lock().unwrap();
    for required in [
        Op::FlowsList,
        Op::FlowsHistory,
        Op::DraftGet,
        Op::DraftUpdate,
        Op::GatesStrict,
        Op::FlowsCreate,
        Op::FlowsUpdate,
        Op::FlowsResume,
        Op::FlowsGetRun,
        Op::FlowsCancelRun,
        Op::SuggestionsUpsert,
    ] {
        assert!(
            calls.iter().any(|call| call.operation == required),
            "missing callback {required:?}: {calls:?}"
        );
    }
    let draft_update = calls
        .iter()
        .find(|call| call.operation == Op::DraftUpdate)
        .unwrap();
    assert_eq!(draft_update.payload["id"], "draft-1");
    assert_eq!(draft_update.payload["graph"]["nodes"][0]["name"], "Renamed");
    let create = calls
        .iter()
        .find(|call| call.operation == Op::FlowsCreate)
        .unwrap();
    assert_eq!(create.payload["name"], "New");
    assert_eq!(create.payload["require_approval"], true);
    assert_eq!(create.payload["graph"]["nodes"][0]["id"], "start");
    let update = calls
        .iter()
        .find(|call| call.operation == Op::FlowsUpdate)
        .unwrap();
    assert_eq!(update.payload["id"], "flow-1");
    assert_eq!(update.payload["graph"]["nodes"][0]["name"], "Start");
    let resume = calls
        .iter()
        .find(|call| call.operation == Op::FlowsResume)
        .unwrap();
    assert_eq!(resume.payload["flow_id"], "flow-1");
    assert_eq!(resume.payload["run_id"], "run-1");
    assert_eq!(resume.payload["approve"], json!(["send-node"]));
    assert_eq!(resume.payload["reject"], json!(["archive-node"]));
    let cancel = calls
        .iter()
        .find(|call| call.operation == Op::FlowsCancelRun)
        .unwrap();
    assert_eq!(cancel.payload["run_id"], "run-1");
    let suggestions = calls
        .iter()
        .find(|call| call.operation == Op::SuggestionsUpsert)
        .unwrap();
    assert_eq!(suggestions.payload[0]["title"], "Daily summary");
    Ok(())
}
