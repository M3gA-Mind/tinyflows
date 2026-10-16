use serde_json::json;
use tinyflows::store::types::{
    NoteKind, NoteSource, ProposalStatus, RunExecutor, RunOrigin, RunRecord, RunStatus, RunStep,
    WorkflowNote, WorkflowProposal,
};

use super::*;

fn every_key(value: &Value, found: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            for (key, inner) in map {
                found.push(key.clone());
                if !FREE_FORM.contains(&key.as_str()) {
                    every_key(inner, found);
                }
            }
        }
        Value::Array(items) => items.iter().for_each(|item| every_key(item, found)),
        _ => {}
    }
}

fn run() -> RunRecord {
    let mut inputs = serde_json::Map::new();
    inputs.insert("someKey".into(), json!({ "a.b": 1, "$set": [1, 2] }));
    RunRecord {
        id: "run-1".into(),
        workflow_id: "sweep".into(),
        status: RunStatus::Succeeded,
        started_at: 1_700_000_000_123,
        finished_at: Some(1_700_000_000_456),
        inputs,
        trigger: Some(json!({ "camelCase": { "inner_snake": true } })),
        origin: Some(RunOrigin {
            kind: "chat".into(),
            session: Some("s".into()),
            label: Some("l".into()),
            workspace: Some("w".into()),
        }),
        executor: Some(RunExecutor {
            host: "box".into(),
            pid: 42,
            started_at_secs: Some(1_700_000_000),
        }),
        cancel_requested: true,
        steps: vec![RunStep {
            node_id: "greet".into(),
            status: "success".into(),
            duration_ms: 12,
            input: Some(json!({ "promptText": "hi" })),
            output: Some(json!([{ "fooBar": 1 }])),
            diagnostics: vec!["args.to".into()],
            transcript: vec![tinyflows::store::types::TranscriptEntry {
                at_ms: 5,
                kind: "tool_call".into(),
                text: "ran".into(),
            }],
        }],
        pending_approvals: vec!["a".into()],
        error: Some("boom".into()),
        summary: Some("done".into()),
        diagnosis: Some(tinyflows::diagnostics::Diagnosis {
            null_bindings: vec![tinyflows::diagnostics::NullBinding {
                node_id: "greet".into(),
                location: "args.to".into(),
                expression: "=.x".into(),
                unverifiable: true,
                reads_from: Some("t".into()),
                suggestion: "fix".into(),
            }],
            empty_prompts: vec!["a".into()],
            hidden_errors: vec![tinyflows::diagnostics::HiddenError {
                node_id: "n".into(),
                message: Some("m".into()),
            }],
            never_ran: vec![tinyflows::diagnostics::NeverRan {
                node_id: "n".into(),
                routed_by: Some("c".into()),
            }],
        }),
    }
}

#[test]
fn a_run_is_stored_as_snake_case_with_millisecond_times() {
    let body = run_body(&run()).unwrap();
    let mut keys = Vec::new();
    every_key(&body, &mut keys);
    for key in &keys {
        assert!(
            key.chars().all(|c| !c.is_ascii_uppercase()),
            "`{key}` is not snake_case in {body}"
        );
    }
    assert_eq!(body["started_ms"], 1_700_000_000_123_u64);
    assert_eq!(body["finished_ms"], 1_700_000_000_456_u64);
    assert_eq!(
        body["executor"]["process_started_ms"],
        1_700_000_000_000_u64
    );
    assert_eq!(body["steps"][0]["transcript"][0]["at_ms"], 5);
    assert_eq!(body["settled"], true);
    assert!(body.get("started_at").is_none() && body.get("startedAt").is_none());
}

#[test]
fn free_form_run_content_is_stored_untouched() {
    let body = run_body(&run()).unwrap();
    assert_eq!(body["inputs"]["someKey"]["a.b"], 1);
    assert_eq!(body["inputs"]["someKey"]["$set"], json!([1, 2]));
    assert_eq!(body["trigger"]["camelCase"]["inner_snake"], true);
    assert_eq!(body["steps"][0]["input"]["promptText"], "hi");
    assert_eq!(body["steps"][0]["output"][0]["fooBar"], 1);
}

#[test]
fn a_run_round_trips_through_its_stored_form() {
    let want = run();
    let got = run_from_body(&run_body(&want).unwrap()).unwrap();
    assert_eq!(got, want);
}

#[test]
fn a_run_without_optional_fields_round_trips() {
    let want = tinyflows::store::new_run_record("r", "w", 7);
    let got = run_from_body(&run_body(&want).unwrap()).unwrap();
    assert_eq!(got, want);
}

fn note() -> WorkflowNote {
    WorkflowNote {
        id: "n1".into(),
        workflow_id: "sweep".into(),
        kind: NoteKind::Rejection,
        text: "no".into(),
        recorded_at: 99,
        source: NoteSource::Agent {
            model: Some("m".into()),
        },
        run_ids: vec!["r1".into()],
        superseded_by: Some("n2".into()),
        pinned: true,
    }
}

#[test]
fn a_journal_is_one_object_with_a_list_of_snake_case_notes() {
    let body = journal_body("sweep", &[note()]);
    assert_eq!(body["workflow_id"], "sweep");
    assert_eq!(body["notes"][0]["recorded_ms"], 99);
    assert_eq!(body["notes"][0]["run_ids"], json!(["r1"]));
    assert_eq!(body["notes"][0]["superseded_by"], "n2");
    assert_eq!(body["notes"][0]["source"]["kind"], "agent");
    assert_eq!(journal_from_body(&body).unwrap(), vec![note()]);
}

#[test]
fn a_journal_without_a_notes_list_is_unreadable() {
    assert!(journal_from_body(&json!({ "workflow_id": "w", "notes_json": "[]" })).is_err());
}

fn proposal() -> WorkflowProposal {
    WorkflowProposal {
        id: "p1".into(),
        workflow_id: "sweep".into(),
        created_at: 5,
        rationale: "because".into(),
        ops: json!([{ "opKind": "set", "path": "a.b" }]),
        evidence_runs: vec!["r1".into()],
        note_ids: vec!["n1".into()],
        base_fingerprint: "abc".into(),
        verification: Some(tinyflows::store::types::ProposalVerification {
            ok: true,
            verified_at: 6,
            messages: vec!["fine".into()],
            diagnosis: None,
        }),
        status: ProposalStatus::Rejected,
        decided_at: Some(8),
        decision_reason: Some("no".into()),
    }
}

#[test]
fn a_proposal_round_trips_and_keeps_its_ops_verbatim() {
    let body = proposal_body(&proposal()).unwrap();
    assert_eq!(body["created_ms"], 5);
    assert_eq!(body["decided_ms"], 8);
    assert_eq!(body["verification"]["verified_ms"], 6);
    assert_eq!(body["base_fingerprint"], "abc");
    assert_eq!(body["ops"][0]["opKind"], "set");
    assert_eq!(proposal_from_body(&body).unwrap(), proposal());
}
