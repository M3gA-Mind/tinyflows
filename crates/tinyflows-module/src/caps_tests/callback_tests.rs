use super::*;

use serde_json::json;
use std::collections::BTreeMap;
use tinyflows::{
    caps::{
        AgentInput, AgentModelSelection, AgentRunIdentity, AgentRunOutcome, AgentRunRequest,
        AgentRunner, AgentUsage, ApprovalProvider, ApprovalRequest, ApprovalSubject, CodeLanguage,
        CodeRunner, ContextBlock, HttpClient, LlmProvider, MemoryProvider, ShellInterpreter,
        ShellRequest, ShellRunner, ShellScript, StopReason, ToolDescriptor, ToolInvoker,
        WorkdirCheck,
    },
    model::{AgentDefinition, AgentLimits, ContextSource, ContextSourceKind, ToolGrant},
    transcript::TranscriptEntry,
};
use tinyflows_bus::runs;

fn ports(connection: Connection, path: &str) -> CapsHostPorts {
    CapsHostPorts {
        connection,
        address: runs::CallbackAddress {
            destination: "test.CapsHost".into(),
            object_path: path.into(),
        },
    }
}

#[test]
fn agent_run_and_outcome_wire_mirrors_match_engine_serde_shapes() {
    let engine_request = AgentRunRequest {
        agent: AgentDefinition {
            id: "researcher".into(),
            limits: AgentLimits {
                max_steps: Some(4),
                max_tool_calls: Some(8),
                agent_timeout_secs: Some(30),
                tool_timeout_secs: Some(5),
            },
            tools: vec![ToolGrant {
                slug: "gh.*".into(),
                connection_ref: Some("github".into()),
            }],
            context: vec![ContextSource::new(ContextSourceKind::Memory {
                scope: "flow".into(),
                query: "project".into(),
                limit: Some(3),
            })],
            ..AgentDefinition::default()
        },
        model: AgentModelSelection {
            model: Some("model".into()),
            provider: None,
        },
        input: AgentInput {
            text: "summarize".into(),
            data: vec![json!({"id":1})],
        },
        context: vec![ContextBlock::text("notes", "host", "keep this")],
        tools: vec![ToolDescriptor {
            slug: "gh.read".into(),
            name: Some("Read".into()),
            description: None,
            input_schema: Some(json!({"type":"object"})),
            connection_ref: Some("github".into()),
        }],
        connection_ref: Some("github".into()),
        working_dir: Some("/workspace".into()),
        identity: AgentRunIdentity {
            run_id: Some("run-1".into()),
            workflow_id: Some("flow-1".into()),
            node_id: "agent-1".into(),
            depth: 2,
            item_index: Some(1),
        },
        metadata: Default::default(),
        output_schema: Some(json!({"type":"object"})),
        config: json!({"prompt":"summarize"}),
    };
    let engine_json = serde_json::to_value(&engine_request).unwrap();
    let wire_request: runs::AgentRunRequest = convert(engine_request).unwrap();
    assert_eq!(serde_json::to_value(wire_request).unwrap(), engine_json);

    let engine_outcome = AgentRunOutcome {
        stop: StopReason::Paused {
            token: Some("resume-1".into()),
            reason: "approval".into(),
            payload: json!({"node":"approve"}),
        },
        text: Some("partial".into()),
        json: json!({"partial":true}),
        raw: json!({"native":"payload"}),
        usage: Some(AgentUsage {
            input_tokens: Some(3),
            output_tokens: Some(5),
            steps: Some(2),
            tool_calls: Some(1),
        }),
        transcript: vec![TranscriptEntry::bounded(12, "tool_call", "gh.read")],
    };
    let engine_json = serde_json::to_value(&engine_outcome).unwrap();
    let wire_outcome: runs::AgentRunOutcome = convert(engine_outcome).unwrap();
    assert_eq!(serde_json::to_value(wire_outcome).unwrap(), engine_json);
}

#[tokio::test]
async fn every_callback_trait_uses_the_exact_host_object_and_preserves_typed_inputs() {
    let (module, host) = support::memory_bus().await;
    let calls = support::serve_caps(&host, "alice", "/caps/alice", false).await;
    let port = ports(module, "/caps/alice");

    assert_eq!(
        LlmProvider::complete(&port, json!({"messages":[1]}), Some("conn"))
            .await
            .unwrap(),
        json!({"owner":"alice","request":{"messages":[1]},"conn":"conn"})
    );
    assert_eq!(
        ToolInvoker::invoke(&port, "gh.read", json!({"x":1}), None)
            .await
            .unwrap(),
        json!({"owner":"alice","slug":"gh.read","args":{"x":1},"conn":null})
    );
    assert_eq!(
        HttpClient::request(&port, json!({"url":"https://example.test"}), Some("http"))
            .await
            .unwrap(),
        json!({"owner":"alice","request":{"url":"https://example.test"},"conn":"http"})
    );

    let source = "return input['x'] + 1";
    let input = json!({"x":4});
    assert_eq!(
        CodeRunner::run(&port, CodeLanguage::Python, source, input.clone())
            .await
            .unwrap(),
        json!({"language":"python","source":source,"input":input})
    );
    let shell = ShellRequest {
        interpreter: ShellInterpreter::Bash,
        script: ShellScript::Inline("printf legacy".into()),
        cwd: Some("/repo".into()),
        env: BTreeMap::from([("A".into(), "B".into())]),
        input: json!({"x":1}),
    };
    let shell_result = ShellRunner::run(&port, shell).await.unwrap();
    assert_eq!(shell_result.exit_code, 7);
    assert_eq!(shell_result.stdout, "printf legacy");
    assert_eq!(shell_result.stderr, "script error");

    assert_eq!(
        MemoryProvider::recall(&port, "flow", "q", json!({"operation":"search"}))
            .await
            .unwrap(),
        json!({"scope":"flow","query":"q","opts":{"operation":"search"}})
    );
    assert_eq!(
        MemoryProvider::flavour(&port, "tone").await.unwrap(),
        json!({"slug":"tone"})
    );
    assert_eq!(
        MemoryProvider::people(&port, None).await.unwrap(),
        json!({"query":null})
    );
    MemoryProvider::remember(&port, "flow", "k", json!(1))
        .await
        .unwrap();
    MemoryProvider::forget(&port, "flow", "k").await.unwrap();

    let approval = ApprovalRequest {
        request_id: "same-request".into(),
        node_id: "review".into(),
        run_id: Some("run-1".into()),
        title: None,
        prompt: None,
        subject: ApprovalSubject {
            kind: "text".into(),
            value: json!("review"),
        },
        assignees: vec![],
        metadata: json!({}),
    };
    assert!(matches!(
        ApprovalProvider::decide(&port, &approval).await.unwrap(),
        tinyflows::caps::ApprovalOutcome::Pending
    ));
    assert!(matches!(
        ApprovalProvider::decide(&port, &approval).await.unwrap(),
        tinyflows::caps::ApprovalOutcome::Decided(_)
    ));
    ApprovalProvider::cancel(&port, "same-request", "settled")
        .await
        .unwrap();

    let request = AgentRunRequest {
        agent: AgentDefinition::new("known"),
        model: AgentModelSelection::default(),
        input: AgentInput {
            text: "do work".into(),
            data: vec![],
        },
        context: vec![],
        tools: vec![],
        connection_ref: None,
        working_dir: None,
        identity: AgentRunIdentity {
            node_id: "agent-node".into(),
            ..AgentRunIdentity::default()
        },
        metadata: Default::default(),
        output_schema: None,
        config: json!({"prompt":"do work"}),
    };
    let outcome = AgentRunner::run(&port, request).await.unwrap();
    assert!(outcome.is_finished());
    assert_eq!(outcome.text.as_deref(), Some("finished"));
    assert_eq!(outcome.transcript.len(), 1);
    assert_eq!(outcome.transcript[0].kind, "agent_final");
    assert_eq!(
        outcome.transcript[0].text,
        "authoritative terminal transcript"
    );

    assert_eq!(
        AgentRunner::resolve_agent(&port, "known")
            .await
            .unwrap()
            .unwrap()
            .id,
        "known"
    );
    assert!(
        AgentRunner::resolve_agent(&port, "missing")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(AgentRunner::list_agents(&port).await.unwrap().len(), 1);
    assert_eq!(
        AgentRunner::resolve_context(&port, "identity", &json!({}), &Default::default())
            .await
            .unwrap()
            .unwrap()
            .text
            .as_deref(),
        Some("alice")
    );
    assert!(
        AgentRunner::resolve_context(&port, "unknown", &json!({}), &Default::default())
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        AgentRunner::resolve_tools(&port, &[], None)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        AgentRunner::resolve_workdir(&port, "/workspace", "repo").await,
        WorkdirCheck::Resolved("/workspace/repo".into())
    );
    assert_eq!(
        AgentRunner::resolve_workdir(&port, "/workspace", "blocked").await,
        WorkdirCheck::Refused("outside workspace".into())
    );
    assert_eq!(
        AgentRunner::resolve_workdir(&port, "/workspace", "other").await,
        WorkdirCheck::Unmanaged
    );

    let calls = calls.lock().unwrap().clone();
    let methods: std::collections::HashSet<_> = calls
        .iter()
        .map(|call| call.split_once(':').unwrap().1)
        .collect();
    assert_eq!(methods, runs::CAPS_METHODS.iter().copied().collect());
    assert_eq!(
        calls
            .iter()
            .filter(|call| call.as_str() == "alice:AgentEvents")
            .count(),
        2
    );
}

#[tokio::test]
async fn callback_addresses_do_not_fall_back_between_owner_scopes() {
    let (module, host) = support::memory_bus().await;
    support::serve_caps(&host, "alice", "/caps/alice", false).await;
    support::serve_caps(&host, "bob", "/caps/bob", false).await;
    let alice = ports(module.clone(), "/caps/alice");
    let bob = ports(module, "/caps/bob");

    assert_eq!(
        LlmProvider::complete(&alice, json!({}), None)
            .await
            .unwrap()["owner"],
        "alice"
    );
    assert_eq!(
        LlmProvider::complete(&bob, json!({}), None).await.unwrap()["owner"],
        "bob"
    );
    let missing = ports(alice.connection.clone(), "/caps/missing");
    let error = LlmProvider::complete(&missing, json!({}), None)
        .await
        .unwrap_err();
    assert!(matches!(error, EngineError::Capability(_)));
}

#[tokio::test]
async fn approval_transport_failure_is_a_capability_error_and_never_pending() {
    let (module, host) = support::memory_bus().await;
    support::serve_caps(&host, "alice", "/caps/fail", true).await;
    let port = ports(module, "/caps/fail");
    let request = ApprovalRequest {
        request_id: "r".into(),
        node_id: "n".into(),
        run_id: None,
        title: None,
        prompt: None,
        subject: ApprovalSubject {
            kind: "text".into(),
            value: json!("x"),
        },
        assignees: vec![],
        metadata: json!({}),
    };
    let error = ApprovalProvider::decide(&port, &request).await.unwrap_err();
    assert!(matches!(error, EngineError::Capability(_)));
}

#[tokio::test]
async fn missing_callback_service_fails_each_result_capability_as_an_error() {
    let (module, _host) = support::memory_bus().await;
    let port = ports(module, "/caps/missing");
    let shell = ShellRequest {
        interpreter: ShellInterpreter::Sh,
        script: ShellScript::Inline("true".into()),
        cwd: None,
        env: BTreeMap::new(),
        input: Value::Null,
    };
    let approval = ApprovalRequest {
        request_id: "r".into(),
        node_id: "n".into(),
        run_id: None,
        title: None,
        prompt: None,
        subject: ApprovalSubject {
            kind: "text".into(),
            value: json!("x"),
        },
        assignees: vec![],
        metadata: json!({}),
    };
    let agent = AgentRunRequest {
        agent: AgentDefinition::new("agent"),
        model: AgentModelSelection::default(),
        input: AgentInput::default(),
        context: vec![],
        tools: vec![],
        connection_ref: None,
        working_dir: None,
        identity: AgentRunIdentity {
            node_id: "agent".into(),
            ..AgentRunIdentity::default()
        },
        metadata: Default::default(),
        output_schema: None,
        config: json!({}),
    };
    let failures = [
        LlmProvider::complete(&port, json!({}), None)
            .await
            .unwrap_err(),
        ToolInvoker::invoke(&port, "tool", json!({}), None)
            .await
            .unwrap_err(),
        HttpClient::request(&port, json!({}), None)
            .await
            .unwrap_err(),
        CodeRunner::run(&port, CodeLanguage::Python, "return input", json!({}))
            .await
            .unwrap_err(),
        ShellRunner::run(&port, shell).await.unwrap_err(),
        MemoryProvider::recall(&port, "flow", "q", json!({}))
            .await
            .unwrap_err(),
        MemoryProvider::flavour(&port, "x").await.unwrap_err(),
        MemoryProvider::people(&port, None).await.unwrap_err(),
        MemoryProvider::remember(&port, "flow", "k", json!(1))
            .await
            .unwrap_err(),
        MemoryProvider::forget(&port, "flow", "k")
            .await
            .unwrap_err(),
        ApprovalProvider::decide(&port, &approval)
            .await
            .unwrap_err(),
        ApprovalProvider::cancel(&port, "r", "done")
            .await
            .unwrap_err(),
        AgentRunner::run_agent(&port, "agent", json!({}), None)
            .await
            .unwrap_err(),
        AgentRunner::run(&port, agent).await.unwrap_err(),
        AgentRunner::resolve_agent(&port, "agent")
            .await
            .unwrap_err(),
        AgentRunner::list_agents(&port).await.unwrap_err(),
        AgentRunner::resolve_context(&port, "host", &json!({}), &Default::default())
            .await
            .unwrap_err(),
        AgentRunner::resolve_tools(&port, &[], None)
            .await
            .unwrap_err(),
    ];
    assert!(
        failures
            .iter()
            .all(|error| matches!(error, EngineError::Capability(_)))
    );
    assert!(matches!(
        AgentRunner::resolve_workdir(&port, "/workspace", "repo").await,
        WorkdirCheck::Refused(_)
    ));
    assert!(
        port.call_one::<_, runs::AgentEventsResponse>(
            runs::caps::AGENT_EVENTS,
            &runs::AgentEventsRequest {
                handle: "h".into(),
                cursor: 0,
            },
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn agent_event_gap_is_rejected_and_terminal_empty_page_is_valid() {
    let (module, host) = support::memory_bus().await;
    support::serve_caps_with_first_event_seq(&host, "alice", "/caps/gap", 2).await;
    let port = ports(module, "/caps/gap");
    let request = AgentRunRequest {
        agent: AgentDefinition::new("agent"),
        model: AgentModelSelection::default(),
        input: AgentInput::default(),
        context: vec![],
        tools: vec![],
        connection_ref: None,
        working_dir: None,
        identity: AgentRunIdentity {
            node_id: "agent".into(),
            ..AgentRunIdentity::default()
        },
        metadata: Default::default(),
        output_schema: None,
        config: json!({}),
    };
    let error = AgentRunner::run(&port, request).await.unwrap_err();
    assert!(matches!(error, EngineError::Capability(_)));
}
