use serde_json::{Value, json};
use std::sync::atomic::{AtomicUsize, Ordering};
use tinybus::{Connection, Proxy};

struct ToolPortHost {
    calls: std::sync::Arc<std::sync::Mutex<Vec<tinyflows_bus::copilot::PortCallRequest>>>,
}

struct BrokenPermissionHost(&'static str);

#[tinybus::interface(name = "ai.tinyhumans.tinyflows.CopilotHost")]
impl BrokenPermissionHost {
    async fn call_port(
        &self,
        request: tinyflows_bus::copilot::PortCallRequest,
    ) -> tinybus::Result<Value> {
        match (self.0, request.operation) {
            (
                "approval-malformed",
                tinyflows_bus::copilot::PortOperation::GatesApprovalManifest,
            ) => Ok(json!({"unexpected":"shape"})),
            (
                "approval-unavailable",
                tinyflows_bus::copilot::PortOperation::GatesApprovalManifest,
            ) => Err(tinybus::Error::failed("callback unavailable")),
            ("strict-malformed", tinyflows_bus::copilot::PortOperation::GatesStrict) => {
                Ok(json!({"unexpected":"not boolean"}))
            }
            _ => Err(tinybus::Error::failed("unexpected callback request")),
        }
    }
}

#[tinybus::interface(name = "ai.tinyhumans.tinyflows.CopilotHost")]
impl ToolPortHost {
    async fn call_port(
        &self,
        request: tinyflows_bus::copilot::PortCallRequest,
    ) -> tinybus::Result<Value> {
        use tinyflows_bus::copilot::PortOperation as Op;
        self.calls.lock().unwrap().push(request.clone());
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
    ) -> tinybus::Result<Value> {
        Ok(json!({"type":"workflow_proposal","name":request.name,"persisted":false}))
    }

    async fn run_flow(
        &self,
        request: tinyflows_bus::copilot::RunFlowRequest,
    ) -> tinybus::Result<Value> {
        Ok(json!({"run_id":format!("run-{}",request.flow_id)}))
    }
}

struct ScopedPreflight(std::sync::Arc<AtomicUsize>);

#[tinybus::interface(name = "ai.tinyhumans.tinyflows.CapsHost")]
impl ScopedPreflight {
    async fn tool_invoke(
        &self,
        _slug: String,
        _args: Value,
        _connection: Option<String>,
    ) -> tinybus::Result<Value> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(json!({"host_canary":"preflight output is ignored"}))
    }
}

pub async fn verify_port_calls(connection: &Connection, proxy: &Proxy) {
    let calls = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let preflight_calls = std::sync::Arc::new(AtomicUsize::new(0));
    connection
        .serve_at(
            "/copilot_port_harness".try_into().unwrap(),
            ToolPortHost {
                calls: calls.clone(),
            },
        )
        .await
        .unwrap();
    connection
        .serve_at(
            "/copilot_port_harness".try_into().unwrap(),
            ScopedPreflight(preflight_calls.clone()),
        )
        .await
        .unwrap();
    connection
        .request_name("test.CopilotPortHarness")
        .await
        .unwrap();
    let host = tinyflows_bus::copilot::CopilotHost {
        bus_name: "test.CopilotPortHarness".into(),
        object_path: "/copilot_port_harness".into(),
    };
    let graph =
        json!({"nodes":[{"id":"start","name":"Start","kind":"trigger","config":{}}],"edges":[]});
    for (name, arguments) in [
        ("list_flows", json!({})),
        ("get_flow_history", json!({"flow_id":"flow-1"})),
        (
            "edit_workflow",
            json!({"draft_id":"draft-1","ops":[{"op":"set_node_name","id":"start","name":"Renamed"}]}),
        ),
        (
            "create_workflow",
            json!({"name":"New","graph":graph.clone()}),
        ),
        (
            "save_workflow",
            json!({"flow_id":"flow-1","graph":graph.clone()}),
        ),
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
        let result: Value = proxy
            .call(
                "InvokeTool",
                (tinyflows_bus::copilot::InvokeToolRequest {
                    name: name.into(),
                    arguments,
                    host: host.clone(),
                    node_kinds: vec![],
                },),
            )
            .await
            .unwrap();
        assert_eq!(result["is_error"], false, "{name}: {result}");
        if name == "list_flows" {
            let text = result["content"][0]["text"].as_str().unwrap();
            let flows: Value = serde_json::from_str(text).unwrap();
            assert_eq!(flows["flows"][0]["id"], "flow-1");
        }
        if name == "get_flow_history" {
            let text = result["content"][0]["text"].as_str().unwrap();
            let history: Value = serde_json::from_str(text).unwrap();
            assert_eq!(history["revisions"][0]["id"], "revision-1");
        }
    }
    for required in [
        tinyflows_bus::copilot::PortOperation::FlowsList,
        tinyflows_bus::copilot::PortOperation::FlowsHistory,
        tinyflows_bus::copilot::PortOperation::DraftGet,
        tinyflows_bus::copilot::PortOperation::DraftUpdate,
        tinyflows_bus::copilot::PortOperation::GatesStrict,
        tinyflows_bus::copilot::PortOperation::FlowsCreate,
        tinyflows_bus::copilot::PortOperation::FlowsUpdate,
        tinyflows_bus::copilot::PortOperation::FlowsResume,
        tinyflows_bus::copilot::PortOperation::FlowsGetRun,
        tinyflows_bus::copilot::PortOperation::FlowsCancelRun,
        tinyflows_bus::copilot::PortOperation::SuggestionsUpsert,
    ] {
        assert!(
            calls
                .lock()
                .unwrap()
                .iter()
                .any(|call| call.operation == required),
            "missing {required:?}"
        );
    }
    let recorded = calls.lock().unwrap().clone();
    let edited = recorded
        .iter()
        .find(|call| call.operation == tinyflows_bus::copilot::PortOperation::DraftUpdate)
        .unwrap();
    assert_eq!(edited.payload["graph"]["nodes"][0]["name"], "Renamed");
    let history_request = recorded
        .iter()
        .find(|call| call.operation == tinyflows_bus::copilot::PortOperation::FlowsHistory)
        .unwrap();
    assert_eq!(history_request.payload["id"], "flow-1");
    assert_eq!(history_request.payload["limit"], 20);
    let created = recorded
        .iter()
        .find(|call| call.operation == tinyflows_bus::copilot::PortOperation::FlowsCreate)
        .unwrap();
    assert_eq!(created.payload["name"], "New");
    assert_eq!(created.payload["require_approval"], true);
    let resumed = recorded
        .iter()
        .find(|call| call.operation == tinyflows_bus::copilot::PortOperation::FlowsResume)
        .unwrap();
    assert_eq!(resumed.payload["flow_id"], "flow-1");
    assert_eq!(resumed.payload["run_id"], "run-1");
    assert_eq!(resumed.payload["approve"], json!(["send-node"]));
    assert_eq!(resumed.payload["reject"], json!(["archive-node"]));
    let suggestions = recorded
        .iter()
        .find(|call| call.operation == tinyflows_bus::copilot::PortOperation::SuggestionsUpsert)
        .unwrap();
    assert_eq!(suggestions.payload[0]["title"], "Daily summary");

    for (label, tool, arguments, path) in [
        (
            "approval-malformed",
            "dry_run_workflow",
            json!({"graph":graph.clone()}),
            "/broken_approval_malformed",
        ),
        (
            "approval-unavailable",
            "dry_run_workflow",
            json!({"graph":graph.clone()}),
            "/broken_approval_unavailable",
        ),
        (
            "strict-malformed",
            "create_workflow",
            json!({"name":"Must not persist","graph":graph.clone()}),
            "/broken_strict_malformed",
        ),
    ] {
        let destination = format!("test.Copilot.{}", label.replace('-', "_"));
        connection
            .serve_at(path.try_into().unwrap(), BrokenPermissionHost(label))
            .await
            .unwrap();
        connection.request_name(&destination).await.unwrap();
        let result: Value = proxy
            .call(
                "InvokeTool",
                (tinyflows_bus::copilot::InvokeToolRequest {
                    name: tool.into(),
                    arguments,
                    host: tinyflows_bus::copilot::CopilotHost {
                        bus_name: destination,
                        object_path: path.into(),
                    },
                    node_kinds: vec![],
                },),
            )
            .await
            .unwrap();
        assert_eq!(result["is_error"], true, "{label}: {result}");
        let text = result["content"][0]["text"].as_str().unwrap();
        if label.starts_with("approval-") {
            assert!(
                text.contains("could not classify sandbox permissions"),
                "{text}"
            );
        } else {
            assert!(!text.contains("Could not create flow"), "{text}");
        }
    }

    let effect_graph = json!({
        "nodes":[
            {"id":"trigger","name":"Trigger","kind":"trigger","config":{}},
            {"id":"send","name":"Send","kind":"tool_call","config":{"slug":"provider:send","args":{"text":"hello"}}}
        ],
        "edges":[{"from_node":"trigger","to_node":"send"}]
    });
    let dry_run: Value = proxy
        .call(
            "InvokeTool",
            (tinyflows_bus::copilot::InvokeToolRequest {
                name: "dry_run_workflow".into(),
                arguments: json!({"graph":effect_graph}),
                host,
                node_kinds: vec![],
            },),
        )
        .await
        .unwrap();
    assert_eq!(dry_run["is_error"], false, "{dry_run}");
    assert!(
        preflight_calls.load(Ordering::SeqCst) > 0,
        "scoped CapsHost preflight callback was not called"
    );
    let text = dry_run["content"][0]["text"].as_str().unwrap();
    assert!(
        !text.contains("preflight output is ignored"),
        "module mock output must ignore the host acknowledgement"
    );
}
