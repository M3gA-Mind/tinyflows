//! Unit tests for the workflow data model.
//!
//! These lean on literal JSON rather than round-tripping Rust values wherever a
//! wire shape is the actual contract: run records and workflow documents are
//! read back from disk by builds other than the one that wrote them, so the
//! spelling of a field is the thing worth asserting.

use serde_json::json;

use super::*;

/// A minimal single-node graph, enough to build a record around.
fn graph() -> crate::model::WorkflowGraph {
    serde_json::from_value(json!({
        "nodes": [{
            "id": "start",
            "kind": "trigger",
            "name": "start",
            "config": { "trigger_kind": "manual" }
        }],
        "edges": []
    }))
    .expect("the fixture graph should parse")
}

fn record() -> WorkflowRecord {
    WorkflowRecord {
        id: "demo".into(),
        name: "Demo".into(),
        description: "A demo workflow".into(),
        enabled: true,
        defaults: Default::default(),
        graph: graph(),
        source_path: None,
    }
}

#[test]
fn summary_counts_nodes_and_reads_the_trigger_kind() {
    let summary = record().summary();

    assert_eq!(summary.id, "demo");
    assert_eq!(summary.node_count, 1);
    assert_eq!(summary.trigger_kind.as_deref(), Some("manual"));
}

#[test]
fn a_document_without_enabled_defaults_to_enabled() {
    let parsed: WorkflowRecord = serde_json::from_value(json!({
        "id": "demo",
        "name": "Demo",
        "graph": graph(),
    }))
    .expect("a document may omit `enabled` and `description`");

    assert!(parsed.enabled);
    assert_eq!(parsed.description, "");
}

#[test]
fn run_status_settles_everything_but_running_and_pending() {
    assert!(!RunStatus::Running.is_settled());
    assert!(!RunStatus::PendingApproval.is_settled());
    for status in [
        RunStatus::Succeeded,
        RunStatus::Failed,
        RunStatus::Cancelled,
        RunStatus::Interrupted,
    ] {
        assert!(status.is_settled(), "{status:?} should be settled");
    }
}

#[test]
fn run_records_use_camel_case_on_the_wire() {
    let wire = serde_json::to_value(RunRecord {
        id: "run-1".into(),
        workflow_id: "demo".into(),
        status: RunStatus::Succeeded,
        started_at: 1,
        finished_at: Some(2),
        steps: vec![RunStep {
            node_id: "start".into(),
            status: "ok".into(),
            duration_ms: 3,
            input: Some(json!("inspect this")),
            output: None,
            diagnostics: Vec::new(),
            transcript: Vec::new(),
        }],
        pending_approvals: Vec::new(),
        error: None,
        inputs: Default::default(),
        trigger: None,
        origin: None,
        executor: None,
        cancel_requested: false,
        summary: None,
        diagnosis: None,
    })
    .expect("a run record should serialize");

    assert!(wire.get("workflowId").is_some());
    assert!(wire.get("startedAt").is_some());
    assert_eq!(wire["status"], json!("succeeded"));
    assert!(
        wire.get("error").is_none(),
        "an absent error should not be written"
    );
    assert!(wire["steps"][0].get("nodeId").is_some());
    assert_eq!(wire["steps"][0]["input"], json!("inspect this"));
}

#[test]
fn a_run_file_written_before_evidence_existed_still_parses() {
    // A literal, not a round trip: run records are read back by builds other
    // than the one that wrote them, and every one of these files is already on
    // operators' disks. If `summary` or `diagnosis` ever stops defaulting, this
    // is the test that says so rather than a support ticket.
    let parsed: RunRecord = serde_json::from_value(json!({
        "id": "run-old",
        "workflowId": "demo",
        "status": "failed",
        "startedAt": 1,
        "finishedAt": 2,
        "steps": [{ "nodeId": "start", "status": "error", "durationMs": 3 }],
        "pendingApprovals": [],
        "error": "boom"
    }))
    .expect("a run record from before the evidence fields must still load");

    assert_eq!(parsed.status, RunStatus::Failed);
    assert!(parsed.steps[0].input.is_none());
    assert!(parsed.summary.is_none());
    assert!(parsed.diagnosis.is_none());
}

#[test]
fn durable_step_evidence_is_bounded_without_changing_small_values() {
    let small = json!({ "answer": "still structured" });
    assert_eq!(bounded_evidence(&small), small);

    let large = json!({ "body": "x".repeat(crate::evidence::MAX_EVIDENCE_BYTES * 2) });
    let bounded = bounded_evidence(&large);
    assert_eq!(bounded[TRUNCATED_KEY], true);
    assert!(is_truncated(&bounded));
    assert!(
        bounded["originalBytes"].as_u64().unwrap() > crate::evidence::MAX_EVIDENCE_BYTES as u64
    );
    assert!(
        serde_json::to_vec(&bounded).unwrap().len() <= crate::evidence::MAX_EVIDENCE_BYTES,
        "the persisted summary itself must remain bounded"
    );

    let escaping = json!({ "body": "\\\"".repeat(crate::evidence::MAX_EVIDENCE_BYTES) });
    assert!(
        serde_json::to_vec(&bounded_evidence(&escaping))
            .unwrap()
            .len()
            <= crate::evidence::MAX_EVIDENCE_BYTES
    );
}

#[test]
fn run_evidence_is_omitted_from_the_wire_when_absent() {
    // The other half of the compatibility bargain: a record with no evidence
    // must not start writing null keys into files an older build reads.
    let wire = serde_json::to_value(RunRecord {
        id: "run-1".into(),
        workflow_id: "demo".into(),
        status: RunStatus::Running,
        started_at: 1,
        finished_at: None,
        steps: Vec::new(),
        pending_approvals: Vec::new(),
        error: None,
        inputs: Default::default(),
        trigger: None,
        origin: None,
        executor: None,
        cancel_requested: false,
        summary: None,
        diagnosis: None,
    })
    .expect("a run record should serialize");

    assert!(wire.get("summary").is_none());
    assert!(wire.get("diagnosis").is_none());
    assert!(wire.get("inputs").is_none());
    assert!(wire.get("trigger").is_none());
    assert!(wire.get("origin").is_none());
}

#[test]
fn what_a_run_was_started_with_survives_the_wire() {
    let record = RunRecord {
        id: "run-1".into(),
        workflow_id: "demo".into(),
        status: RunStatus::Running,
        started_at: 1,
        finished_at: None,
        steps: Vec::new(),
        pending_approvals: Vec::new(),
        error: None,
        inputs: Default::default(),
        trigger: None,
        origin: None,
        executor: None,
        cancel_requested: false,
        summary: None,
        diagnosis: None,
    }
    .with_inputs(
        &json!({ "repo": "acme/api" }).as_object().cloned().unwrap(),
        &json!({ "event": "push" }),
    )
    .with_origin(Some(RunOrigin::session("pty-1").in_workspace("/tmp/work")));

    let wire = serde_json::to_value(&record).expect("a run record should serialize");
    assert_eq!(wire["inputs"]["repo"], json!("acme/api"));
    assert_eq!(wire["trigger"]["event"], json!("push"));
    assert_eq!(wire["origin"]["kind"], json!("session"));
    assert_eq!(wire["origin"]["session"], json!("pty-1"));
    assert_eq!(wire["origin"]["workspace"], json!("/tmp/work"));

    let back: RunRecord = serde_json::from_value(wire).expect("and parse back");
    assert_eq!(back, record);
}

#[test]
fn an_oversized_input_is_summarized_rather_than_carried_whole() {
    let record = crate::store::new_run_record("run-1", "demo", 1).with_inputs(
        &json!({ "body": "x".repeat(run::MAX_INPUT_BYTES * 3) })
            .as_object()
            .cloned()
            .unwrap(),
        &json!({}),
    );
    assert_eq!(record.inputs["body"]["_flowsTruncated"], json!(true));
    assert!(
        serde_json::to_vec(&record.inputs["body"]).unwrap().len() <= run::MAX_INPUT_BYTES,
        "one oversized input must not bloat every listing that shows it"
    );
}

#[test]
fn an_empty_trigger_is_not_recorded_as_a_value() {
    let record = crate::store::new_run_record("run-1", "demo", 1)
        .with_inputs(&Default::default(), &json!({}));
    assert!(record.trigger.is_none());
}

#[test]
fn a_record_written_before_the_marker_was_renamed_still_reads_as_truncated() {
    // Run records are written once and never revised, so files carrying the old
    // key exist. A reader that only knew the new one would render the wrapper
    // as if it were the value — an object whose fields are `originalBytes` and
    // `preview` — rather than as an elision.
    let legacy = serde_json::json!({
        LEGACY_TRUNCATED_KEY: true,
        "originalBytes": 200_000,
        "preview": "{\"items\":[",
    });

    assert!(is_truncated(&legacy));
}

#[test]
fn an_ordinary_value_is_not_mistaken_for_a_truncation_wrapper() {
    // Both directions matter: a stored value that merely *has* the key set to
    // something other than `true` is a value, not a wrapper.
    assert!(!is_truncated(&serde_json::json!({ "items": [1, 2, 3] })));
    assert!(!is_truncated(&serde_json::json!({ TRUNCATED_KEY: false })));
    assert!(!is_truncated(&serde_json::json!("a string")));
}

#[test]
fn an_executor_and_a_cancel_request_survive_a_json_round_trip() {
    let mut record = crate::store::new_run_record("owned", "alpha", 100).with_executor(Some(
        RunExecutor {
            host: "somewhere".to_string(),
            pid: 4711,
            started_at_secs: Some(1_700_000_000),
        },
    ));
    record.cancel_requested = true;

    let json = serde_json::to_string(&record).expect("serializes");
    let read: RunRecord = serde_json::from_str(&json).expect("parses");

    assert_eq!(read.executor, record.executor);
    assert!(read.cancel_requested);
}

#[test]
fn a_record_written_before_executors_existed_still_parses() {
    // Both fields are additive, so an older run record must keep loading — and
    // must read as unowned, which is what makes a sweep treat it as an orphan.
    let older = serde_json::json!({
        "id": "legacy",
        "workflowId": "alpha",
        "status": "running",
        "startedAt": 100,
    });

    let record: RunRecord = serde_json::from_value(older).expect("older records still parse");

    assert!(record.executor.is_none());
    assert!(!record.cancel_requested);
}

fn proposal(workflow_id: &str, created_at: u64) -> WorkflowProposal {
    WorkflowProposal {
        id: crate::store::mint_proposal_id(created_at),
        workflow_id: workflow_id.to_string(),
        created_at,
        rationale: "the timeout is too short for a cold cache".into(),
        ops: json!([{ "op": "update_node_config", "id": "build", "config": { "timeout": 600 } }]),
        evidence_runs: vec!["run-1".into()],
        note_ids: Vec::new(),
        base_fingerprint: "abc123".into(),
        verification: None,
        status: ProposalStatus::Pending,
        decided_at: None,
        decision_reason: None,
    }
}

#[test]
fn only_a_verified_pending_proposal_is_applicable() {
    let mut subject = proposal("sweep", 1);
    assert!(
        !subject.is_applicable(),
        "an unverified proposal must not be offered"
    );

    subject.verification = Some(ProposalVerification {
        ok: false,
        verified_at: 2,
        messages: vec!["node 'build' does not exist".into()],
        diagnosis: None,
    });
    assert!(
        !subject.is_applicable(),
        "a proposal that failed its check is evidence, not an offer"
    );

    subject.verification = Some(ProposalVerification {
        ok: true,
        verified_at: 3,
        messages: Vec::new(),
        diagnosis: None,
    });
    assert!(subject.is_applicable());

    subject.status = ProposalStatus::Accepted;
    assert!(
        !subject.is_applicable(),
        "a proposal cannot be applied twice"
    );
}

#[test]
fn fingerprints_distinguish_graphs_and_agree_with_themselves() {
    let graph: crate::model::WorkflowGraph = serde_json::from_value(json!({
        "nodes": [{ "id": "t", "kind": "trigger", "name": "start" }],
        "edges": []
    }))
    .expect("the fixture graph should parse");
    let changed: crate::model::WorkflowGraph = serde_json::from_value(json!({
        "nodes": [{ "id": "t", "kind": "trigger", "name": "start again" }],
        "edges": []
    }))
    .expect("the fixture graph should parse");

    assert_eq!(fingerprint(&graph), fingerprint(&graph));
    assert_ne!(
        fingerprint(&graph),
        fingerprint(&changed),
        "a graph that moved must not look unchanged to an accept"
    );
}
