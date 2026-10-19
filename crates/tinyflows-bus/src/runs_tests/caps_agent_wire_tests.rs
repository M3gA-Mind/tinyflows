use super::*;

#[test]
fn agent_capability_records_round_trip_and_match_fixtures() {
    pin(AgentLimits::default(), serde_json::json!({}));
    pin(
        ToolGrant {
            slug: "gh.*".into(),
            connection_ref: Some("conn".into()),
        },
        serde_json::json!({"slug":"gh.*","connection_ref":"conn"}),
    );
    pin(
        ContextSourceKind::Text {
            text: serde_json::json!("hello"),
        },
        serde_json::json!({"kind":"text","text":"hello"}),
    );
    pin(
        ContextSourceKind::Items,
        serde_json::json!({"kind":"items"}),
    );
    pin(
        ContextSourceKind::Memory {
            scope: "flow".into(),
            query: "q".into(),
            limit: None,
        },
        serde_json::json!({"kind":"memory","scope":"flow","query":"q"}),
    );
    pin(
        ContextSourceKind::Flavour {
            slug: "tone".into(),
        },
        serde_json::json!({"kind":"flavour","slug":"tone"}),
    );
    pin(
        ContextSourceKind::Host {
            source: "identity".into(),
            params: serde_json::Value::Null,
        },
        serde_json::json!({"kind":"host","source":"identity"}),
    );
    pin(
        ContextSource {
            label: None,
            kind: ContextSourceKind::Items,
            optional: false,
        },
        serde_json::json!({"kind":"items"}),
    );
    pin(
        AgentDefinition::default(),
        serde_json::json!({"id":"","name":"","description":"","instructions":null,"model":null,"provider":null,"limits":{},"tools":[],"context":[],"metadata":{}}),
    );
    pin(AgentModelSelection::default(), serde_json::json!({}));
    pin(AgentInput::default(), serde_json::json!({"text":""}));
    pin(
        ContextBlock::default(),
        serde_json::json!({"label":"","source_kind":""}),
    );
    pin(
        ToolDescriptor {
            slug: "read".into(),
            ..ToolDescriptor::default()
        },
        serde_json::json!({"slug":"read"}),
    );
    pin(
        AgentRunIdentity {
            node_id: "agent".into(),
            ..AgentRunIdentity::default()
        },
        serde_json::json!({"node_id":"agent","depth":0}),
    );
    pin(
        AgentRunRequest {
            client_request_id: "request-1".into(),
            agent: AgentDefinition {
                id: "researcher".into(),
                ..AgentDefinition::default()
            },
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
            config: serde_json::json!({"prompt":"hi"}),
        },
        serde_json::json!({"client_request_id":"request-1","agent":{"id":"researcher","name":"","description":"","instructions":null,"model":null,"provider":null,"limits":{},"tools":[],"context":[],"metadata":{}},"model":{},"input":{"text":""},"context":[],"tools":[],"identity":{"node_id":"agent","depth":0},"config":{"prompt":"hi"}}),
    );
    pin(
        AgentCancelRequest {
            client_request_id: "request-1".into(),
        },
        serde_json::json!({"client_request_id":"request-1"}),
    );
    let legacy: AgentRunRequest = serde_json::from_value(serde_json::json!({
        "agent":{"id":"legacy"},
        "config":null
    }))
    .unwrap();
    assert_eq!(legacy.client_request_id, "");
    pin(
        AgentRunHandle {
            handle: "h1".into(),
        },
        serde_json::json!({"handle":"h1"}),
    );
    pin(
        AgentEventsRequest {
            handle: "h1".into(),
            cursor: 0,
        },
        serde_json::json!({"handle":"h1","cursor":0}),
    );
    pin(
        AgentProgressEvent {
            seq: 1,
            at_ms: 2,
            kind: "tool_call".into(),
            text: "read".into(),
        },
        serde_json::json!({"seq":1,"at_ms":2,"kind":"tool_call","text":"read"}),
    );
    pin(
        AgentEventsResponse {
            events: vec![],
            done: true,
            outcome: Some(AgentRunOutcome {
                stop: StopReason::Finished,
                text: None,
                json: serde_json::Value::Null,
                raw: serde_json::Value::Null,
                usage: None,
                transcript: vec![],
            }),
        },
        serde_json::json!({"events":[],"done":true,"outcome":{"stop":{"stop":"finished"},"json":null,"raw":null}}),
    );
    pin(StopReason::Finished, serde_json::json!({"stop":"finished"}));
    pin(
        StopReason::LimitStop {
            limit: "steps".into(),
        },
        serde_json::json!({"stop":"limit_stop","limit":"steps"}),
    );
    pin(
        StopReason::Paused {
            token: None,
            reason: "approval".into(),
            payload: serde_json::Value::Null,
        },
        serde_json::json!({"stop":"paused","reason":"approval"}),
    );
    pin(AgentUsage::default(), serde_json::json!({}));
    pin(
        AgentRunOutcome {
            stop: StopReason::Finished,
            text: Some("done".into()),
            json: serde_json::json!({"ok":true}),
            raw: serde_json::json!("raw"),
            usage: None,
            transcript: vec![],
        },
        serde_json::json!({"stop":{"stop":"finished"},"text":"done","json":{"ok":true},"raw":"raw"}),
    );
    pin(
        TranscriptEntry {
            at_ms: 3,
            kind: "message".into(),
            text: "hi".into(),
        },
        serde_json::json!({"atMs":3,"kind":"message","text":"hi"}),
    );
    pin(
        WorkdirCheck::Unmanaged,
        serde_json::json!({"status":"unmanaged"}),
    );
    pin(
        WorkdirCheck::Resolved("/work".into()),
        serde_json::json!({"status":"resolved","path":"/work"}),
    );
    pin(
        WorkdirCheck::Refused("outside".into()),
        serde_json::json!({"status":"refused","path":"outside"}),
    );
    pin(
        ResolveContextRequest {
            source: "identity".into(),
            params: serde_json::json!({}),
            identity: AgentRunIdentity {
                node_id: "agent".into(),
                ..AgentRunIdentity::default()
            },
        },
        serde_json::json!({"source":"identity","params":{},"identity":{"node_id":"agent","depth":0}}),
    );
    pin(
        ResolveToolsRequest {
            grants: vec![],
            connection_ref: None,
        },
        serde_json::json!({"grants":[]}),
    );
    pin(
        ResolveWorkdirRequest {
            workspace: "/w".into(),
            declared: "repo".into(),
        },
        serde_json::json!({"workspace":"/w","declared":"repo"}),
    );
}
