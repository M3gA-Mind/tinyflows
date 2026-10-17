use super::*;

use serde_json::json;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tinybus::Connection;
use tinyflows_bus::runs::{
    self, AgentDefinition, AgentEventsRequest, AgentEventsResponse, AgentProgressEvent,
    AgentRunHandle, AgentRunOutcome, ApprovalOutcome, CallbackAddress, CapsHost,
    ResolveContextRequest, ResolveToolsRequest, ResolveWorkdirRequest, ShellRunOutcome,
    WorkdirCheck,
};

pub(super) struct ToolGate {
    pub started: tokio::sync::Notify,
    pub release: tokio::sync::Notify,
    pub blocked_slug: Option<String>,
}

#[derive(Clone)]
pub(super) struct FakeHost {
    pub owner: String,
    pub calls: Arc<Mutex<Vec<String>>>,
    pub approval_calls: Arc<AtomicUsize>,
    pub fail_approval: bool,
    pub tool_gate: Option<Arc<ToolGate>>,
    pub first_event_seq: u64,
    pub terminal_transcript_len: usize,
}

impl FakeHost {
    pub fn record(&self, member: &str) {
        self.calls
            .lock()
            .unwrap()
            .push(format!("{}:{member}", self.owner));
    }
}

#[tinybus::interface(name = "ai.tinyhumans.tinyflows.CapsHost")]
impl FakeHost {
    async fn llm_complete(&self, request: Value, conn: Option<String>) -> tinybus::Result<Value> {
        self.record("LlmComplete");
        Ok(json!({"owner": self.owner, "request": request, "conn": conn}))
    }
    async fn tool_invoke(
        &self,
        slug: String,
        args: Value,
        conn: Option<String>,
    ) -> tinybus::Result<Value> {
        self.record("ToolInvoke");
        if let Some(gate) = &self.tool_gate {
            if gate
                .blocked_slug
                .as_deref()
                .is_none_or(|blocked_slug| blocked_slug == slug)
            {
                gate.started.notify_one();
                gate.release.notified().await;
            } else if slug == "nested.release" {
                gate.release.notify_one();
            }
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
        Ok(ShellRunOutcome {
            exit_code: 7,
            stdout: request.script_name(),
            stderr: "script error".into(),
        })
    }
    async fn memory_recall(
        &self,
        scope: String,
        query: String,
        opts: Value,
    ) -> tinybus::Result<Value> {
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
    async fn memory_remember(
        &self,
        _scope: String,
        _key: String,
        _value: Value,
    ) -> tinybus::Result<()> {
        self.record("MemoryRemember");
        Ok(())
    }
    async fn memory_forget(&self, _scope: String, _key: String) -> tinybus::Result<()> {
        self.record("MemoryForget");
        Ok(())
    }
    async fn approval_decide(
        &self,
        _request: runs::ApprovalRequest,
    ) -> tinybus::Result<ApprovalOutcome> {
        self.record("ApprovalDecide");
        if self.fail_approval {
            return Err(tinybus::Error::failed("approval provider unavailable"));
        }
        let call = self.approval_calls.fetch_add(1, Ordering::SeqCst);
        if call == 0 {
            Ok(ApprovalOutcome::Pending)
        } else {
            Ok(ApprovalOutcome::Decided(runs::ApprovalDecision {
                approved: true,
                decided_by: Some(self.owner.clone()),
                comment: None,
                payload: None,
            }))
        }
    }
    async fn approval_cancel(&self, _request: runs::ApprovalCancelRequest) -> tinybus::Result<()> {
        self.record("ApprovalCancel");
        Ok(())
    }
    async fn agent_run(&self, request: runs::AgentRunRequest) -> tinybus::Result<AgentRunHandle> {
        self.record("AgentRun");
        Ok(AgentRunHandle {
            handle: format!("{}:{}", self.owner, request.identity.node_id),
        })
    }
    async fn agent_events(
        &self,
        request: AgentEventsRequest,
    ) -> tinybus::Result<AgentEventsResponse> {
        self.record("AgentEvents");
        if request.cursor == 0 {
            Ok(AgentEventsResponse {
                events: vec![AgentProgressEvent {
                    seq: self.first_event_seq,
                    at_ms: 10,
                    kind: "tool_call".into(),
                    text: "read".into(),
                }],
                done: false,
                outcome: None,
            })
        } else {
            let transcript = (0..self.terminal_transcript_len)
                .map(|index| runs::TranscriptEntry {
                    at_ms: 11 + index as i64,
                    kind: "agent_final".into(),
                    text: if index == 0 {
                        "authoritative terminal transcript".into()
                    } else {
                        "additional host transcript".into()
                    },
                })
                .collect();
            Ok(AgentEventsResponse {
                events: vec![],
                done: true,
                outcome: Some(AgentRunOutcome {
                    stop: runs::StopReason::Finished,
                    text: Some("finished".into()),
                    json: json!({"ok": true}),
                    raw: json!({"ok": true}),
                    usage: None,
                    transcript,
                }),
            })
        }
    }
    async fn resolve_agent(&self, id: String) -> tinybus::Result<Option<AgentDefinition>> {
        self.record("ResolveAgent");
        Ok((id == "known").then(|| AgentDefinition {
            id,
            ..AgentDefinition::default()
        }))
    }
    async fn list_agents(&self) -> tinybus::Result<Vec<AgentDefinition>> {
        self.record("ListAgents");
        Ok(vec![AgentDefinition {
            id: "known".into(),
            ..AgentDefinition::default()
        }])
    }
    async fn resolve_context(
        &self,
        request: ResolveContextRequest,
    ) -> tinybus::Result<Option<runs::ContextBlock>> {
        self.record("ResolveContext");
        Ok((request.source == "identity").then(|| runs::ContextBlock {
            label: "identity".into(),
            source_kind: "host".into(),
            text: Some(self.owner.clone()),
            data: Value::Null,
        }))
    }
    async fn resolve_tools(
        &self,
        _request: ResolveToolsRequest,
    ) -> tinybus::Result<Vec<runs::ToolDescriptor>> {
        self.record("ResolveTools");
        Ok(vec![])
    }
    async fn resolve_workdir(
        &self,
        request: ResolveWorkdirRequest,
    ) -> tinybus::Result<WorkdirCheck> {
        self.record("ResolveWorkdir");
        Ok(if request.declared == "blocked" {
            WorkdirCheck::Refused("outside workspace".into())
        } else if request.declared == "repo" {
            WorkdirCheck::Resolved(format!("{}/repo", request.workspace))
        } else {
            WorkdirCheck::Unmanaged
        })
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
    let module = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let host = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    host.request_name("test.CapsHost").await.unwrap();
    (module, host)
}

pub(super) async fn serve_caps(
    host: &Connection,
    owner: &str,
    path: &str,
    fail_approval: bool,
) -> Arc<Mutex<Vec<String>>> {
    let calls = Arc::new(Mutex::new(Vec::new()));
    host.serve_at(
        path.parse().unwrap(),
        FakeHost {
            owner: owner.into(),
            calls: calls.clone(),
            approval_calls: Arc::new(AtomicUsize::new(0)),
            fail_approval,
            tool_gate: None,
            first_event_seq: 1,
            terminal_transcript_len: 1,
        },
    )
    .await
    .unwrap();
    calls
}

pub(super) async fn serve_caps_with_blocked_tools(
    host: &Connection,
    owner: &str,
    path: &str,
) -> Arc<ToolGate> {
    let gate = Arc::new(ToolGate {
        started: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
        blocked_slug: None,
    });
    host.serve_at(
        path.parse().unwrap(),
        FakeHost {
            owner: owner.into(),
            calls: Arc::new(Mutex::new(Vec::new())),
            approval_calls: Arc::new(AtomicUsize::new(0)),
            fail_approval: false,
            tool_gate: Some(Arc::clone(&gate)),
            first_event_seq: 1,
            terminal_transcript_len: 1,
        },
    )
    .await
    .unwrap();
    gate
}

pub(super) async fn serve_caps_with_nested_barrier(
    host: &Connection,
    owner: &str,
    path: &str,
) -> Arc<ToolGate> {
    let gate = Arc::new(ToolGate {
        started: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
        blocked_slug: Some("nested.blocked".into()),
    });
    host.serve_at(
        path.parse().unwrap(),
        FakeHost {
            owner: owner.into(),
            calls: Arc::new(Mutex::new(Vec::new())),
            approval_calls: Arc::new(AtomicUsize::new(0)),
            fail_approval: false,
            tool_gate: Some(Arc::clone(&gate)),
            first_event_seq: 1,
            terminal_transcript_len: 1,
        },
    )
    .await
    .unwrap();
    gate
}

pub(super) async fn serve_caps_with_first_event_seq(
    host: &Connection,
    owner: &str,
    path: &str,
    first_event_seq: u64,
) {
    host.serve_at(
        path.parse().unwrap(),
        FakeHost {
            owner: owner.into(),
            calls: Arc::new(Mutex::new(Vec::new())),
            approval_calls: Arc::new(AtomicUsize::new(0)),
            fail_approval: false,
            tool_gate: None,
            first_event_seq,
            terminal_transcript_len: 1,
        },
    )
    .await
    .unwrap();
}

pub(super) async fn serve_caps_with_terminal_transcript_len(
    host: &Connection,
    owner: &str,
    path: &str,
    terminal_transcript_len: usize,
) {
    host.serve_at(
        path.parse().unwrap(),
        FakeHost {
            owner: owner.into(),
            calls: Arc::new(Mutex::new(Vec::new())),
            approval_calls: Arc::new(AtomicUsize::new(0)),
            fail_approval: false,
            tool_gate: None,
            first_event_seq: 1,
            terminal_transcript_len,
        },
    )
    .await
    .unwrap();
}

pub(super) fn caps_host(path: &str) -> CapsHost {
    CapsHost {
        callback: CallbackAddress {
            destination: "test.CapsHost".into(),
            object_path: path.into(),
        },
    }
}

pub(super) fn unsafe_child_graph() -> Value {
    json!({
        "nodes": [
            {"id":"start","kind":"trigger","name":"Trigger"},
            {"id":"outer","kind":"condition","name":"Outer","config":{"field":"o"}},
            {"id":"inner","kind":"condition","name":"Inner","config":{"field":"i"}},
            {"id":"outer_else","kind":"output_parser","name":"Outer else"},
            {"id":"inner_else","kind":"output_parser","name":"Inner else"},
            {"id":"a","kind":"output_parser","name":"A"},
            {"id":"c","kind":"output_parser","name":"C"},
            {"id":"m","kind":"merge","name":"Merge"}
        ],
        "edges": [
            {"from_node":"start","from_port":"main","to_node":"outer"},
            {"from_node":"start","from_port":"main","to_node":"c"},
            {"from_node":"outer","from_port":"true","to_node":"inner"},
            {"from_node":"outer","from_port":"false","to_node":"outer_else"},
            {"from_node":"inner","from_port":"true","to_node":"a"},
            {"from_node":"inner","from_port":"false","to_node":"inner_else"},
            {"from_node":"a","from_port":"main","to_node":"m"},
            {"from_node":"c","from_port":"main","to_node":"m"}
        ]
    })
}
