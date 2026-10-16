use super::*;

#[test]
fn non_agent_capability_records_round_trip_and_match_fixtures() {
    pin(
        CodeLanguage::JavaScript,
        serde_json::json!("java_script"),
    );
    pin(
        CodeRunRequest { language: CodeLanguage::Python, source: "return input".into(), input: serde_json::json!({"x": 1}) },
        serde_json::json!({"language":"python","source":"return input","input":{"x":1}}),
    );
    pin(
        ShellInterpreter::Bash,
        serde_json::json!("bash"),
    );
    pin(
        ShellScript::Inline("printf ok".into()),
        serde_json::json!({"kind":"inline","value":"printf ok"}),
    );
    pin(
        ShellScript::Path("scripts/run.sh".into()),
        serde_json::json!({"kind":"path","value":"scripts/run.sh"}),
    );
    pin(
        ShellRunRequest {
            interpreter: ShellInterpreter::Sh,
            script: ShellScript::Inline("cat".into()),
            cwd: Some("/workspace".into()),
            env: [("LANG".to_owned(), "C".to_owned())].into_iter().collect(),
            input: serde_json::json!([{"json": {"n": 1}}]),
        },
        serde_json::json!({"interpreter":"sh","script":{"kind":"inline","value":"cat"},"cwd":"/workspace","env":{"LANG":"C"},"input":[{"json":{"n":1}}]}),
    );
    pin(
        ShellRunOutcome { exit_code: 7, stdout: "out".into(), stderr: "err".into() },
        serde_json::json!({"exit_code":7,"stdout":"out","stderr":"err"}),
    );
    pin(
        MemoryRecallRequest { scope: "flow".into(), query: "q".into(), opts: serde_json::json!({"operation":"search"}) },
        serde_json::json!({"scope":"flow","query":"q","opts":{"operation":"search"}}),
    );
    pin(MemoryFlavourRequest { slug: "concise".into() }, serde_json::json!({"slug":"concise"}));
    pin(MemoryPeopleRequest { query: None }, serde_json::json!({"query":null}));
    pin(
        MemoryRememberRequest { scope: "flow".into(), key: "k".into(), value: serde_json::json!(3) },
        serde_json::json!({"scope":"flow","key":"k","value":3}),
    );
    pin(MemoryForgetRequest { scope: "flow".into(), key: "k".into() }, serde_json::json!({"scope":"flow","key":"k"}));
    pin(
        ApprovalSubject { kind: "text".into(), value: serde_json::json!("review me") },
        serde_json::json!({"kind":"text","value":"review me"}),
    );
    pin(
        ApprovalRequest {
            request_id: "req-1".into(), node_id: "approve".into(), run_id: Some("run-1".into()),
            title: Some("Review".into()), prompt: None,
            subject: ApprovalSubject { kind: "json".into(), value: serde_json::json!({"n": 1}) },
            assignees: vec!["owner".into()], metadata: serde_json::json!({}),
        },
        serde_json::json!({"request_id":"req-1","node_id":"approve","run_id":"run-1","title":"Review","prompt":null,"subject":{"kind":"json","value":{"n":1}},"assignees":["owner"],"metadata":{}}),
    );
    pin(
        ApprovalDecision { approved: false, decided_by: None, comment: Some("no".into()), payload: None },
        serde_json::json!({"approved":false,"decided_by":null,"comment":"no","payload":null}),
    );
    pin(ApprovalOutcome::Pending, serde_json::json!({"status":"pending"}));
    pin(
        ApprovalOutcome::Decided(ApprovalDecision { approved: true, decided_by: None, comment: None, payload: None }),
        serde_json::json!({"status":"decided","decision":{"approved":true,"decided_by":null,"comment":null,"payload":null}}),
    );
    pin(
        ApprovalCancelRequest { request_id: "req-1".into(), reason: "cancelled".into() },
        serde_json::json!({"request_id":"req-1","reason":"cancelled"}),
    );
}
