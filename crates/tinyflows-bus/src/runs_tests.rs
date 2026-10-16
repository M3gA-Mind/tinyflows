use super::*;

fn pin<T>(value: T, expected: serde_json::Value)
where
    T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
{
    let actual = serde_json::to_value(&value).unwrap();
    assert_eq!(actual, expected);
    assert_eq!(serde_json::from_value::<T>(expected).unwrap(), value);
}

#[test]
fn run_control_records_keep_their_literal_wire_shape() {
    pin(
        StartRun {
            graph: serde_json::json!({"nodes": [], "edges": []}),
            trigger: serde_json::json!({"source": "manual"}),
            run_id: Some("run-1".into()),
        },
        serde_json::json!({
            "graph": {"nodes": [], "edges": []},
            "trigger": {"source": "manual"},
            "run_id": "run-1"
        }),
    );
    pin(
        ResumeRun { run_id: "run-1".into(), approvals: vec!["n1".into()] },
        serde_json::json!({"run_id":"run-1","approvals":["n1"]}),
    );
    pin(
        RunEvent { seq: 1, kind: "step".into(), data: serde_json::json!({"node":"n1"}) },
        serde_json::json!({"seq":1,"kind":"step","data":{"node":"n1"}}),
    );
    pin(
        PollRun { events: vec![], done: true },
        serde_json::json!({"events":[],"done":true}),
    );
    pin(
        CallbackAddress { destination: "host.cb".into(), object_path: "/run/1".into() },
        serde_json::json!({"destination":"host.cb","object_path":"/run/1"}),
    );
    pin(
        CapsHost { callback: CallbackAddress { destination: "host.cb".into(), object_path: "/run/1".into() } },
        serde_json::json!({"callback":{"destination":"host.cb","object_path":"/run/1"}}),
    );
    assert_eq!(
        CAPS_METHODS,
        ["LlmComplete", "ToolInvoke", "HttpRequest", "CodeRun", "ShellRun", "MemoryRecall", "MemoryFlavour", "MemoryPeople", "MemoryRemember", "MemoryForget", "AgentRun", "AgentEvents", "ResolveAgent", "ListAgents", "ResolveContext", "ResolveTools", "ResolveWorkdir", "ApprovalDecide", "ApprovalCancel"]
    );
    assert_eq!(CAPS_CONTRACT_VERSION, 1);
    assert_eq!(MAX_AGENT_EVENTS_PAGE, 256);
    assert_eq!(MAX_AGENT_TRANSCRIPT_EVENTS, 10_000);
}

#[cfg(test)]
#[path = "runs_tests/caps_wire_tests.rs"]
mod caps_wire_tests;

#[cfg(test)]
#[path = "runs_tests/caps_agent_wire_tests.rs"]
mod caps_agent_wire_tests;
