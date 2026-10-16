use super::*;

use std::sync::{Arc, Mutex, atomic::{AtomicUsize, Ordering}};
use tinyflows_bus::runs::{
    self, AgentDefinition, AgentEventsRequest, AgentEventsResponse, AgentProgressEvent,
    AgentRunHandle, AgentRunOutcome, ApprovalOutcome, CallbackAddress, CapsHost,
    ResolveContextRequest, ResolveToolsRequest, ResolveWorkdirRequest, ShellRunOutcome,
    WorkdirCheck,
};
use tinybus::Connection;

pub(super) struct ToolGate {
    pub started: tokio::sync::Notify,
    pub release: tokio::sync::Notify,
}

#[derive(Clone)]
pub(super) struct FakeHost {
    pub owner: String,
    pub calls: Arc<Mutex<Vec<String>>>,
    pub approval_calls: Arc<AtomicUsize>,
    pub fail_approval: bool,
    pub tool_gate: Option<Arc<ToolGate>>,
}

impl FakeHost {
    pub fn record(&self, member: &str) {
        self.calls.lock().unwrap().push(format!("{}:{member}", self.owner));
    }
}

#[tinybus::interface(name = "ai.tinyhumans.tinyflows.CapsHost")]
impl FakeHost {
    async fn llm_complete(&self, request: Value, conn: Option<String>) -> tinybus::Result<Value> {
        self.record("LlmComplete");
        Ok(json!({"owner": self.owner, "request": request, "conn": conn}))
    }
    async fn tool_invoke(&self, slug: String, args: Value, conn: Option<String>) -> tinybus::Result<Value> {
        self.record("ToolInvoke");
        if let Some(gate) = &self.tool_gate {
            gate.started.notify_one();
            gate.release.notified().await;
        }
        Ok(json!({"owner": self.owner, "slug": slug, "args": args, "conn": conn}))
    }
    async fn http_request(&self, request: Value, conn: Option<String>) -> tinybus::Result<Value> {
        self.record("HttpRequest");
        Ok(json!({"owner": self.owner, "request": request, "conn": conn}))
    }
    async fn code_run(&self, request: runs::CodeRunRequest) -> tinybus::Result<Value> {
        self.record("CodeRun");
        Ok(json!({"language": request.language, "source": request.source, "input": request.input}))
    }
    async fn shell_run(&self, request: runs::ShellRunRequest) -> tinybus::Result<ShellRunOutcome> {
        self.record("ShellRun");
        Ok(ShellRunOutcome { exit_code: 7, stdout: format!("{}", request.script_name()), stderr: "script error".into() })
    }
    async fn memory_recall(&self, scope: String, query: String, opts: Value) -> tinybus::Result<Value> {
        self.record("MemoryRecall");
        Ok(json!({"scope": scope, "query": query, "opts": opts}))
    }
    async fn memory_flavour(&self, slug: String) -> tinybus::Result<Value> {
        self.record("MemoryFlavour");
        Ok(json!({"slug": slug}))
    }
    async fn memory_people(&self, query: Option<String>) -> tinybus::Result<Value> {
        self.record("MemoryPeople");
        Ok(json!({"query": query}))
    }
    async fn memory_remember(&self, _scope: String, _key: String, _value: Value) -> tinybus::Result<()> {
        self.record("MemoryRemember");
        Ok(())
    }
    async fn memory_forget(&self, _scope: String, _key: String) -> tinybus::Result<()> {
        self.record("MemoryForget");
        Ok(())
    }
    async fn approval_decide(&self, _request: runs::ApprovalRequest) -> tinybus::Result<ApprovalOutcome> {
        self.record("ApprovalDecide");
        if self.fail_approval {
            return Err(tinybus::Error::failed("approval provider unavailable"));
        }
        let call = self.approval_calls.fetch_add(1, Ordering::SeqCst);
        if call == 0 {
            Ok(ApprovalOutcome::Pending)
        } else {
            Ok(ApprovalOutcome::Decided(runs::ApprovalDecision {
                approved: true, decided_by: Some(self.owner.clone()), comment: None, payload: None,
            }))
        }
    }
    async fn approval_cancel(&self, _request_id: String, _reason: String) -> tinybus::Result<()> {
        self.record("ApprovalCancel");
        Ok(())
    }
    async fn agent_run(&self, request: runs::AgentRunRequest) -> tinybus::Result<AgentRunHandle> {
        self.record("AgentRun");
        Ok(AgentRunHandle { handle: format!("{}:{}", self.owner, request.identity.node_id) })
    }
    async fn agent_events(&self, request: AgentEventsRequest) -> tinybus::Result<AgentEventsResponse> {
        self.record("AgentEvents");
        if request.cursor == 0 {
            Ok(AgentEventsResponse {
                events: vec![AgentProgressEvent { seq: 1, at_ms: 10, kind: "tool_call".into(), text: "read".into() }],
                done: false,
                outcome: None,
            })
        } else {
            Ok(AgentEventsResponse {
                events: vec![],
                done: true,
                outcome: Some(AgentRunOutcome {
                    stop: runs::StopReason::Finished,
                    text: Some("finished".into()),
                    json: json!({"ok": true}),
                    raw: json!({"ok": true}),
                    usage: None,
                    transcript: vec![runs::TranscriptEntry {
                        at_ms: 11,
                        kind: "agent_final".into(),
                        text: "authoritative terminal transcript".into(),
                    }],
                }),
            })
        }
    }
    async fn resolve_agent(&self, id: String) -> tinybus::Result<Option<AgentDefinition>> {
        self.record("ResolveAgent");
        Ok((id == "known").then(|| AgentDefinition { id, ..AgentDefinition::default() }))
    }
    async fn list_agents(&self) -> tinybus::Result<Vec<AgentDefinition>> {
        self.record("ListAgents");
        Ok(vec![AgentDefinition { id: "known".into(), ..AgentDefinition::default() }])
    }
    async fn resolve_context(&self, request: ResolveContextRequest) -> tinybus::Result<Option<runs::ContextBlock>> {
        self.record("ResolveContext");
        Ok((request.source == "identity").then(|| runs::ContextBlock {
            label: "identity".into(), source_kind: "host".into(), text: Some(self.owner.clone()), data: Value::Null,
        }))
    }
    async fn resolve_tools(&self, _request: ResolveToolsRequest) -> tinybus::Result<Vec<runs::ToolDescriptor>> {
        self.record("ResolveTools");
        Ok(vec![])
    }
    async fn resolve_workdir(&self, request: ResolveWorkdirRequest) -> tinybus::Result<WorkdirCheck> {
        self.record("ResolveWorkdir");
        Ok(if request.declared == "blocked" { WorkdirCheck::Refused("outside workspace".into()) }
           else if request.declared == "repo" { WorkdirCheck::Resolved(format!("{}/repo", request.workspace)) }
           else { WorkdirCheck::Unmanaged })
    }
}

trait ShellRequestFixture {
    fn script_name(&self) -> String;
}

impl ShellRequestFixture for runs::ShellRunRequest {
    fn script_name(&self) -> String {
        match &self.script {
            runs::ShellScript::Inline(body) => body.clone(),
            runs::ShellScript::Path(path) => path.clone(),
        }
    }
}

pub(super) async fn memory_bus() -> (Connection, Connection) {
    let bus = tinybus::transport::memory::MemoryBus::new();
    tinybus::broker::Broker::new().spawn(bus.clone());
    let module = Connection::connect(bus.connect().await.unwrap()).await.unwrap();
    let host = Connection::connect(bus.connect().await.unwrap()).await.unwrap();
    host.request_name("test.CapsHost").await.unwrap();
    (module, host)
}

pub(super) async fn serve_caps(host: &Connection, owner: &str, path: &str, fail_approval: bool) -> Arc<Mutex<Vec<String>>> {
    let calls = Arc::new(Mutex::new(Vec::new()));
    host.serve_at(
        path.parse().unwrap(),
        FakeHost { owner: owner.into(), calls: calls.clone(), approval_calls: Arc::new(AtomicUsize::new(0)), fail_approval, tool_gate: None },
    ).await.unwrap();
    calls
}

pub(super) async fn serve_caps_with_blocked_tools(host: &Connection, owner: &str, path: &str) -> Arc<ToolGate> {
    let gate = Arc::new(ToolGate { started: tokio::sync::Notify::new(), release: tokio::sync::Notify::new() });
    host.serve_at(
        path.parse().unwrap(),
        FakeHost {
            owner: owner.into(),
            calls: Arc::new(Mutex::new(Vec::new())),
            approval_calls: Arc::new(AtomicUsize::new(0)),
            fail_approval: false,
            tool_gate: Some(Arc::clone(&gate)),
        },
    ).await.unwrap();
    gate
}

pub(super) fn caps_host(path: &str) -> CapsHost {
    CapsHost { callback: CallbackAddress { destination: "test.CapsHost".into(), object_path: path.into() } }
}
