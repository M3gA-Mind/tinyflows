use super::*;

use serde_json::json;
use tinyflows::{
    caps::{
        AgentInput, AgentModelSelection, AgentRunIdentity, AgentRunOutcome, AgentRunRequest,
        AgentRunner, AgentUsage, ContextBlock, StopReason, ToolDescriptor,
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

fn request() -> AgentRunRequest {
    AgentRunRequest {
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
async fn agent_event_gap_is_rejected_and_terminal_empty_page_is_valid() {
    let (module, host) = support::memory_bus().await;
    support::serve_caps_with_first_event_seq(&host, "alice", "/caps/gap", 2).await;
    let port = ports(module, "/caps/gap");
    let error = AgentRunner::run(&port, request()).await.unwrap_err();
    assert!(matches!(error, EngineError::Capability(_)));
}

#[tokio::test]
async fn oversized_authoritative_terminal_transcript_fails_as_a_capability_error() {
    let (module, host) = support::memory_bus().await;
    support::serve_caps_with_terminal_transcript_len(
        &host,
        "alice",
        "/caps/large-transcript",
        runs::MAX_AGENT_TRANSCRIPT_EVENTS + 1,
    )
    .await;
    let port = ports(module, "/caps/large-transcript");
    let error = AgentRunner::run(&port, request()).await.unwrap_err();
    assert!(matches!(error, EngineError::Capability(_)));
}
