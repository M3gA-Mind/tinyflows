use super::*;

use std::collections::BTreeMap;
use tinyflows::{
    caps::{
        AgentInput, AgentModelSelection, AgentRunIdentity, AgentRunRequest, AgentRunner,
        ApprovalProvider, ApprovalRequest, ApprovalSubject, CodeLanguage, CodeRunner,
        HttpClient, LlmProvider, MemoryProvider, ShellInterpreter, ShellRequest,
        ShellRunner, ShellScript, ToolInvoker, WorkdirCheck,
    },
    model::AgentDefinition,
};
use tinyflows_bus::runs::{self, CapsHost};

fn ports(connection: Connection, path: &str) -> CapsHostPorts {
    CapsHostPorts {
        connection,
        address: runs::CallbackAddress {
            destination: "test.CapsHost".into(),
            object_path: path.into(),
        },
    }
}

#[tokio::test]
async fn every_callback_trait_uses_the_exact_host_object_and_preserves_typed_inputs() {
    let (module, host) = support::memory_bus().await;
    let calls = support::serve_caps(&host, "alice", "/caps/alice", false).await;
    let port = ports(module, "/caps/alice");

    assert_eq!(LlmProvider::complete(&port, json!({"messages":[1]}), Some("conn")).await.unwrap(),
        json!({"owner":"alice","request":{"messages":[1]},"conn":"conn"}));
    assert_eq!(ToolInvoker::invoke(&port, "gh.read", json!({"x":1}), None).await.unwrap(),
        json!({"owner":"alice","slug":"gh.read","args":{"x":1},"conn":null}));
    assert_eq!(HttpClient::request(&port, json!({"url":"https://example.test"}), Some("http")).await.unwrap(),
        json!({"owner":"alice","request":{"url":"https://example.test"},"conn":"http"}));

    let source = "return input['x'] + 1";
    let input = json!({"x":4});
    assert_eq!(CodeRunner::run(&port, CodeLanguage::Python, source, input.clone()).await.unwrap(),
        json!({"language":"python","source":source,"input":input}));
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

    assert_eq!(MemoryProvider::recall(&port, "flow", "q", json!({"operation":"search"})).await.unwrap(),
        json!({"scope":"flow","query":"q","opts":{"operation":"search"}}));
    assert_eq!(MemoryProvider::flavour(&port, "tone").await.unwrap(), json!({"slug":"tone"}));
    assert_eq!(MemoryProvider::people(&port, None).await.unwrap(), json!({"query":null}));
    MemoryProvider::remember(&port, "flow", "k", json!(1)).await.unwrap();
    MemoryProvider::forget(&port, "flow", "k").await.unwrap();

    let approval = ApprovalRequest {
        request_id: "same-request".into(), node_id: "review".into(), run_id: Some("run-1".into()),
        title: None, prompt: None, subject: ApprovalSubject { kind: "text".into(), value: json!("review") },
        assignees: vec![], metadata: json!({}),
    };
    assert!(matches!(ApprovalProvider::decide(&port, &approval).await.unwrap(), tinyflows::caps::ApprovalOutcome::Pending));
    assert!(matches!(ApprovalProvider::decide(&port, &approval).await.unwrap(), tinyflows::caps::ApprovalOutcome::Decided(_)));
    ApprovalProvider::cancel(&port, "same-request", "settled").await.unwrap();

    let request = AgentRunRequest {
        agent: AgentDefinition::new("known"), model: AgentModelSelection::default(),
        input: AgentInput { text: "do work".into(), data: vec![] }, context: vec![], tools: vec![],
        connection_ref: None, working_dir: None,
        identity: AgentRunIdentity { node_id: "agent-node".into(), ..AgentRunIdentity::default() },
        metadata: Default::default(), output_schema: None, config: json!({"prompt":"do work"}),
    };
    let outcome = AgentRunner::run(&port, request).await.unwrap();
    assert!(outcome.is_finished());
    assert_eq!(outcome.text.as_deref(), Some("finished"));
    assert_eq!(outcome.transcript.len(), 1);
    assert_eq!(outcome.transcript[0].kind, "agent_final");
    assert_eq!(outcome.transcript[0].text, "authoritative terminal transcript");

    assert_eq!(AgentRunner::resolve_agent(&port, "known").await.unwrap().unwrap().id, "known");
    assert!(AgentRunner::resolve_agent(&port, "missing").await.unwrap().is_none());
    assert_eq!(AgentRunner::list_agents(&port).await.unwrap().len(), 1);
    assert_eq!(AgentRunner::resolve_context(&port, "identity", &json!({}), &Default::default()).await.unwrap().unwrap().text.as_deref(), Some("alice"));
    assert!(AgentRunner::resolve_context(&port, "unknown", &json!({}), &Default::default()).await.unwrap().is_none());
    assert!(AgentRunner::resolve_tools(&port, &[], None).await.unwrap().is_empty());
    assert_eq!(AgentRunner::resolve_workdir(&port, "/workspace", "repo").await, WorkdirCheck::Resolved("/workspace/repo".into()));
    assert_eq!(AgentRunner::resolve_workdir(&port, "/workspace", "blocked").await, WorkdirCheck::Refused("outside workspace".into()));
    assert_eq!(AgentRunner::resolve_workdir(&port, "/workspace", "other").await, WorkdirCheck::Unmanaged);

    let calls = calls.lock().unwrap().clone();
    let methods: std::collections::HashSet<_> = calls
        .iter()
        .map(|call| call.split_once(':').unwrap().1)
        .collect();
    assert_eq!(methods, runs::CAPS_METHODS.iter().copied().collect());
    assert_eq!(calls.iter().filter(|call| call.as_str() == "alice:AgentEvents").count(), 2);
}

#[tokio::test]
async fn callback_addresses_do_not_fall_back_between_owner_scopes() {
    let (module, host) = support::memory_bus().await;
    support::serve_caps(&host, "alice", "/caps/alice", false).await;
    support::serve_caps(&host, "bob", "/caps/bob", false).await;
    let alice = ports(module.clone(), "/caps/alice");
    let bob = ports(module, "/caps/bob");

    assert_eq!(LlmProvider::complete(&alice, json!({}), None).await.unwrap()["owner"], "alice");
    assert_eq!(LlmProvider::complete(&bob, json!({}), None).await.unwrap()["owner"], "bob");
    let missing = ports(alice.connection.clone(), "/caps/missing");
    let error = LlmProvider::complete(&missing, json!({}), None).await.unwrap_err();
    assert!(matches!(error, EngineError::Capability(_)));
}

#[tokio::test]
async fn approval_transport_failure_is_a_capability_error_and_never_pending() {
    let (module, host) = support::memory_bus().await;
    support::serve_caps(&host, "alice", "/caps/fail", true).await;
    let port = ports(module, "/caps/fail");
    let request = ApprovalRequest {
        request_id: "r".into(), node_id: "n".into(), run_id: None,
        title: None, prompt: None, subject: ApprovalSubject { kind: "text".into(), value: json!("x") },
        assignees: vec![], metadata: json!({}),
    };
    let error = ApprovalProvider::decide(&port, &request).await.unwrap_err();
    assert!(matches!(error, EngineError::Capability(_)));
}
