use std::sync::Arc;

use serde_json::{Value, json};
use tinytools::{PermissionLevel, Tool, ToolResult};

use super::fakes::{MemHost, OverlayKinds};
use super::ports::NodeKindSource;
use super::*;

fn host() -> Arc<MemHost> {
    Arc::new(MemHost::default())
}

fn valid_graph() -> Value {
    json!({
        "nodes": [
            { "id": "t", "kind": "trigger", "name": "Manual" },
            { "id": "a", "kind": "agent", "name": "Summarize", "config": { "prompt": "hi" } }
        ],
        "edges": [ { "from_node": "t", "to_node": "a" } ]
    })
}

fn scheduled_graph() -> Value {
    json!({
        "nodes": [
            { "id": "t", "kind": "trigger", "name": "Cron",
              "config": { "trigger_kind": "schedule", "cron": "0 9 * * *" } },
            { "id": "a", "kind": "agent", "name": "Summarize", "config": { "prompt": "hi" } }
        ],
        "edges": [ { "from_node": "t", "to_node": "a" } ]
    })
}

fn text(result: &ToolResult) -> String {
    result.output().to_string()
}

fn json_of(result: &ToolResult) -> Value {
    serde_json::from_str(&text(result)).expect("tool output is JSON")
}

// ── the belt's contract: names, permissions, external effect ─────────────────

#[test]
fn belt_names_permissions_and_effects_are_pinned() {
    let h = host();
    let kinds: Arc<dyn NodeKindSource> = Arc::new(ports::PortableNodeKinds);
    let belt: Vec<(Box<dyn Tool>, &str, PermissionLevel, bool)> = vec![
        (
            Box::new(ReviseWorkflowTool::new(h.clone())),
            "revise_workflow",
            PermissionLevel::None,
            false,
        ),
        (
            Box::new(EditWorkflowTool::new(h.clone(), h.clone(), h.clone())),
            "edit_workflow",
            PermissionLevel::None,
            false,
        ),
        (
            Box::new(ValidateWorkflowTool::new(h.clone(), h.clone(), h.clone())),
            "validate_workflow",
            PermissionLevel::None,
            false,
        ),
        (
            Box::new(DryRunWorkflowTool::new(h.clone(), h.clone(), h.clone())),
            "dry_run_workflow",
            PermissionLevel::None,
            false,
        ),
        (
            Box::new(CreateWorkflowTool::new(h.clone(), h.clone())),
            "create_workflow",
            PermissionLevel::Write,
            true,
        ),
        (
            Box::new(DuplicateFlowTool::new(h.clone())),
            "duplicate_flow",
            PermissionLevel::Write,
            true,
        ),
        (
            Box::new(SaveWorkflowTool::new(h.clone(), h.clone(), h.clone())),
            "save_workflow",
            PermissionLevel::Write,
            false,
        ),
        (
            Box::new(ResumeFlowRunTool::new(h.clone())),
            "resume_flow_run",
            PermissionLevel::Execute,
            true,
        ),
        (
            Box::new(CancelFlowRunTool::new(h.clone())),
            "cancel_flow_run",
            PermissionLevel::Write,
            true,
        ),
        (
            Box::new(GetFlowHistoryTool::new(h.clone())),
            "get_flow_history",
            PermissionLevel::None,
            false,
        ),
        (
            Box::new(ListFlowRunsTool::new(h.clone())),
            "list_flow_runs",
            PermissionLevel::None,
            false,
        ),
        (
            Box::new(ListFlowsTool::new(h.clone())),
            "list_flows",
            PermissionLevel::None,
            false,
        ),
        (
            Box::new(GetFlowTool::new(h.clone())),
            "get_flow",
            PermissionLevel::None,
            false,
        ),
        (
            Box::new(GetFlowRunTool::new(h.clone())),
            "get_flow_run",
            PermissionLevel::None,
            false,
        ),
        (
            Box::new(ListAgentDefinitionsTool::new(h.clone())),
            "list_agent_definitions",
            PermissionLevel::None,
            false,
        ),
        (
            Box::new(ListNodeKindsTool::new(kinds.clone())),
            "list_node_kinds",
            PermissionLevel::None,
            false,
        ),
        (
            Box::new(GetNodeKindContractTool::new(kinds)),
            "get_node_kind_contract",
            PermissionLevel::None,
            false,
        ),
        (
            Box::new(SuggestWorkflowsTool::new(h)),
            "suggest_workflows",
            PermissionLevel::None,
            false,
        ),
    ];
    for (tool, name, permission, effect) in belt {
        assert_eq!(tool.name(), name);
        assert_eq!(tool.permission_level(), permission, "{name} permission");
        assert_eq!(tool.external_effect(), effect, "{name} external_effect");
        assert!(
            tool.parameters_schema().is_object(),
            "{name} schema is an object"
        );
    }
}

// ── edit_workflow ────────────────────────────────────────────────────────────

#[test]
fn edit_op_shape_names_the_fields_each_op_wants() {
    assert_eq!(
        edit_op_shape("add_node"),
        Some("{ op, node: { id, kind, name, config? } }")
    );
    assert!(edit_op_shape("rename_node").unwrap().contains("new_id"));
    assert_eq!(edit_op_shape("explode"), None);
}

fn edit_tool(h: &Arc<MemHost>) -> EditWorkflowTool {
    EditWorkflowTool::new(h.clone(), h.clone(), h.clone())
}

#[tokio::test]
async fn editing_a_flow_id_seeds_a_new_draft_and_never_touches_the_flow() {
    let h = host();
    h.put_flow("flow-1", "Digest", false, valid_graph());
    let result = edit_tool(&h)
        .execute(json!({
            "flow_id": "flow-1",
            "ops": [ { "op": "set_node_name", "id": "a", "name": "Renamed" } ]
        }))
        .await
        .unwrap();
    assert!(!result.is_error, "{}", text(&result));
    let payload = json_of(&result);
    let draft_id = payload["draft_id"].as_str().expect("draft handle");
    assert_eq!(payload["flow_id"], "flow-1");
    assert_eq!(payload["persisted"], false);
    assert!(
        payload["next"]
            .as_str()
            .unwrap()
            .contains("NOT on flow flow-1")
    );

    let drafts = h.drafts.lock().unwrap();
    let draft = drafts.get(draft_id).expect("draft was created");
    assert_eq!(draft.flow_id.as_deref(), Some("flow-1"));
    let renamed = draft.graph["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|n| n["name"] == "Renamed");
    assert!(renamed, "the edit landed on the draft");

    let flows = h.flows.lock().unwrap();
    let saved = flows.get("flow-1").unwrap();
    assert!(
        saved.graph.nodes.iter().all(|n| n.name != "Renamed"),
        "the saved flow is untouched"
    );
}

#[tokio::test]
async fn editing_a_draft_writes_back_to_the_same_draft() {
    let h = host();
    h.put_draft("d1", None, "Draft", valid_graph());
    let result = edit_tool(&h)
        .execute(json!({
            "draft_id": "d1",
            "ops": [ { "op": "set_node_name", "id": "a", "name": "Again" } ]
        }))
        .await
        .unwrap();
    assert!(!result.is_error, "{}", text(&result));
    let payload = json_of(&result);
    assert_eq!(payload["draft_id"], "d1");
    assert!(payload["next"].as_str().unwrap().contains("not yet linked"));
    assert_eq!(
        h.drafts.lock().unwrap().len(),
        1,
        "no second draft is minted"
    );
    assert!(
        h.drafts.lock().unwrap()["d1"]
            .graph
            .to_string()
            .contains("Again")
    );
}

#[tokio::test]
async fn a_failed_draft_write_back_is_reported_not_swallowed() {
    let h = host();
    h.put_draft("d1", None, "Draft", valid_graph());
    *h.fail_draft_update.lock().unwrap() = true;
    let result = edit_tool(&h)
        .execute(json!({
            "draft_id": "d1",
            "ops": [ { "op": "set_node_name", "id": "a", "name": "Lost" } ]
        }))
        .await
        .unwrap();
    assert!(result.is_error);
    let message = text(&result);
    assert!(
        message.contains("could NOT be written back to draft d1"),
        "{message}"
    );
    assert!(message.contains("PREVIOUS graph"), "{message}");
}

#[tokio::test]
async fn a_bad_op_reports_its_index_and_expected_shape() {
    let h = host();
    let result = edit_tool(&h)
        .execute(json!({
            "graph": valid_graph(),
            "ops": [
                { "op": "set_node_name", "id": "a", "name": "ok" },
                { "op": "add_edge" }
            ]
        }))
        .await
        .unwrap();
    assert!(result.is_error);
    let message = text(&result);
    assert!(message.contains("Could not parse op 1"), "{message}");
    assert!(message.contains("op `add_edge` expects"), "{message}");

    let unknown = edit_tool(&h)
        .execute(json!({ "graph": valid_graph(), "ops": [ { "op": "teleport" } ] }))
        .await
        .unwrap();
    assert!(text(&unknown).contains("unknown op type `teleport`"));
}

#[tokio::test]
async fn adding_an_existing_node_id_points_at_the_ordering_fix() {
    let h = host();
    let result = edit_tool(&h)
        .execute(json!({
            "graph": valid_graph(),
            "ops": [ { "op": "add_node", "node": { "id": "a", "kind": "agent", "name": "Dup",
                                                   "config": { "prompt": "x" } } } ]
        }))
        .await
        .unwrap();
    assert!(result.is_error);
    assert!(text(&result).contains("remove_node op for it BEFORE the add_node"));
}

#[tokio::test]
async fn edit_requires_a_base_and_non_empty_ops() {
    let h = host();
    let no_base = edit_tool(&h)
        .execute(json!({ "ops": [ { "op": "remove_node", "id": "a" } ] }))
        .await
        .unwrap();
    assert!(text(&no_base).contains("Provide one of `draft_id`"));
    let empty = edit_tool(&h)
        .execute(json!({ "graph": valid_graph(), "ops": [] }))
        .await
        .unwrap();
    assert!(text(&empty).contains("`ops` is empty"));
}

#[tokio::test]
async fn a_hard_gate_failure_comes_back_as_a_tool_error() {
    let h = host();
    *h.gate_errors.lock().unwrap() = vec!["unresolvable binding".to_string()];
    let result = edit_tool(&h)
        .execute(json!({
            "graph": valid_graph(),
            "ops": [ { "op": "set_node_name", "id": "a", "name": "x" } ]
        }))
        .await
        .unwrap();
    assert!(result.is_error);
    assert!(text(&result).contains("unresolvable binding"));
}

// ── revise / validate / dry run ──────────────────────────────────────────────

#[tokio::test]
async fn revise_workflow_validates_then_proposes_without_persisting() {
    let h = host();
    let tool = ReviseWorkflowTool::new(h.clone());
    let ok = tool
        .execute(json!({ "name": "Digest", "graph": valid_graph(), "instruction": "tighten" }))
        .await
        .unwrap();
    assert!(!ok.is_error, "{}", text(&ok));
    let payload = json_of(&ok);
    assert_eq!(payload["persisted"], false);
    assert_eq!(payload["instruction"], "tighten");
    assert_eq!(payload["require_approval"], true);
    assert!(h.flows.lock().unwrap().is_empty());

    let missing = tool
        .execute(json!({ "graph": valid_graph() }))
        .await
        .unwrap();
    assert!(text(&missing).contains("Missing 'name'"));
    let invalid = tool
        .execute(json!({ "name": "x", "graph": { "nodes": [], "edges": [] } }))
        .await
        .unwrap();
    assert!(invalid.is_error);
    assert!(text(&invalid).contains("Revised workflow graph is invalid"));
}

#[test]
fn validate_report_fails_closed() {
    use super::draft_validate::validate_workflow_report_is_ok;
    assert!(validate_workflow_report_is_ok(true, &[], false));
    assert!(!validate_workflow_report_is_ok(false, &[], false));
    assert!(!validate_workflow_report_is_ok(
        true,
        &["gate".into()],
        false
    ));
    assert!(!validate_workflow_report_is_ok(true, &[], true));
}

#[tokio::test]
async fn validate_workflow_reports_structure_and_gates_without_proposing() {
    let h = host();
    let tool = ValidateWorkflowTool::new(h.clone(), h.clone(), h.clone());
    let ok = tool
        .execute(json!({ "graph": valid_graph() }))
        .await
        .unwrap();
    let report = json_of(&ok);
    assert_eq!(report["ok"], true);
    assert_eq!(report["structurally_valid"], true);

    *h.gate_errors.lock().unwrap() = vec!["bad slug".to_string()];
    let gated = json_of(
        &tool
            .execute(json!({ "graph": valid_graph() }))
            .await
            .unwrap(),
    );
    assert_eq!(gated["ok"], false);
    assert_eq!(gated["gate_errors"], json!(["bad slug"]));

    h.put_draft("d1", None, "D", json!({ "nodes": [], "edges": [] }));
    let broken = json_of(&tool.execute(json!({ "draft_id": "d1" })).await.unwrap());
    assert_eq!(broken["structurally_valid"], false);
    assert_eq!(
        broken["gate_errors"],
        json!([]),
        "gates only run on valid graphs"
    );

    let none = tool.execute(json!({})).await.unwrap();
    assert!(text(&none).contains("Provide one of `draft_id`"));
    let missing_flow = tool.execute(json!({ "flow_id": "nope" })).await.unwrap();
    assert!(text(&missing_flow).contains("flow 'nope' not found"));
}

#[tokio::test]
async fn dry_run_executes_against_the_host_sandbox_and_labels_it() {
    let h = host();
    let tool = DryRunWorkflowTool::new(h.clone(), h.clone(), h.clone());
    let result = tool
        .execute(json!({ "graph": valid_graph(), "input": { "x": 1 } }))
        .await
        .unwrap();
    assert!(!result.is_error, "{}", text(&result));
    let payload = json_of(&result);
    assert_eq!(payload["sandbox"], true);
    assert_eq!(payload["ok"], true);
    assert!(
        payload["note"]
            .as_str()
            .unwrap()
            .contains("NO real side effects")
    );

    let invalid = tool
        .execute(json!({ "graph": { "nodes": [], "edges": [] } }))
        .await
        .unwrap();
    assert!(text(&invalid).contains("Cannot dry-run an invalid graph"));
}

// ── persistence ──────────────────────────────────────────────────────────────

#[test]
fn create_report_is_honest_about_a_failed_force_disable() {
    use super::persistence::create_workflow_report;
    assert!(!create_workflow_report(false, true).0);
    assert!(!create_workflow_report(true, true).0);
    let (enabled, note) = create_workflow_report(true, false);
    assert!(enabled);
    assert!(note.contains("could NOT be force-disabled"));
}

#[tokio::test]
async fn create_workflow_always_leaves_the_flow_disabled() {
    let h = host();
    let tool = CreateWorkflowTool::new(h.clone(), h.clone());
    let result = tool
        .execute(json!({ "name": "Manual flow", "graph": valid_graph() }))
        .await
        .unwrap();
    assert!(!result.is_error, "{}", text(&result));
    let payload = json_of(&result);
    assert_eq!(payload["enabled"], false);
    assert_eq!(payload["type"], "workflow_created");
    let id = payload["flow_id"].as_str().unwrap();
    assert!(
        !h.flows.lock().unwrap()[id].enabled,
        "force-disabled in the store"
    );
}

#[tokio::test]
async fn create_workflow_reports_the_true_state_when_disable_fails() {
    let h = host();
    *h.fail_set_enabled.lock().unwrap() = true;
    let tool = CreateWorkflowTool::new(h.clone(), h.clone());
    let payload = json_of(
        &tool
            .execute(json!({ "name": "Manual flow", "graph": valid_graph() }))
            .await
            .unwrap(),
    );
    assert_eq!(
        payload["enabled"], true,
        "a manual flow is born enabled and stays so"
    );
    assert!(payload["note"].as_str().unwrap().contains("ENABLED"));
}

#[tokio::test]
async fn create_workflow_stops_at_the_strict_gate() {
    let h = host();
    *h.gate_errors.lock().unwrap() = vec!["tool slug is not real".to_string()];
    let tool = CreateWorkflowTool::new(h.clone(), h.clone());
    let result = tool
        .execute(json!({ "name": "x", "graph": valid_graph() }))
        .await
        .unwrap();
    assert!(result.is_error);
    assert!(text(&result).contains("call create_workflow again"));
    assert!(h.flows.lock().unwrap().is_empty());
}

#[tokio::test]
async fn duplicate_flow_returns_a_disabled_copy() {
    let h = host();
    h.put_flow("flow-1", "Digest", true, valid_graph());
    let tool = DuplicateFlowTool::new(h.clone());
    let payload = json_of(&tool.execute(json!({ "flow_id": "flow-1" })).await.unwrap());
    assert_eq!(payload["type"], "workflow_duplicated");
    assert_eq!(payload["name"], "Digest (copy)");
    assert_eq!(payload["enabled"], false);
    let missing = tool.execute(json!({ "flow_id": "ghost" })).await.unwrap();
    assert!(text(&missing).contains("Could not duplicate flow"));
}

#[tokio::test]
async fn save_workflow_prefers_the_draft_and_relays_host_notes() {
    let h = host();
    h.put_flow("flow-1", "Digest", false, valid_graph());
    h.put_draft("d1", Some("flow-1"), "Digest v2", scheduled_graph());
    let tool = SaveWorkflowTool::new(h.clone(), h.clone(), h.clone());
    let result = tool
        .execute(
            json!({ "flow_id": "flow-1", "draft_id": "d1", "graph": valid_graph(),
                         "name": "Digest v2" }),
        )
        .await
        .unwrap();
    assert!(!result.is_error, "{}", text(&result));
    let payload = json_of(&result);
    assert_eq!(payload["type"], "workflow_saved");
    assert_eq!(payload["persisted"], true);
    let warnings: Vec<&str> = payload["warnings"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(Value::as_str)
        .collect();
    assert!(warnings.contains(&"note from host"), "{warnings:?}");
    assert!(
        !warnings.iter().any(|w| w.starts_with("flow updated:")),
        "the boilerplate update line is dropped: {warnings:?}"
    );
    let saved = &h.flows.lock().unwrap()["flow-1"];
    assert_eq!(saved.name, "Digest v2");
    assert_eq!(
        serde_json::to_value(&saved.graph).unwrap()["nodes"][0]["config"]["trigger_kind"],
        "schedule",
        "the draft's graph, not the inline one, was written"
    );
}

#[tokio::test]
async fn save_workflow_warns_loudly_when_it_leaves_an_automatic_flow_live() {
    let h = host();
    h.put_flow("flow-1", "Digest", true, scheduled_graph());
    let tool = SaveWorkflowTool::new(h.clone(), h.clone(), h.clone());
    let payload = json_of(
        &tool
            .execute(json!({ "flow_id": "flow-1", "graph": scheduled_graph() }))
            .await
            .unwrap(),
    );
    assert!(payload["warnings"].as_array().unwrap().iter().any(|w| {
        w.as_str()
            .unwrap()
            .starts_with("WARNING: this flow is ENABLED")
    }));
}

#[tokio::test]
async fn save_workflow_needs_a_target_and_passes_the_gates() {
    let h = host();
    let tool = SaveWorkflowTool::new(h.clone(), h.clone(), h.clone());
    let no_flow = tool
        .execute(json!({ "graph": valid_graph() }))
        .await
        .unwrap();
    assert!(text(&no_flow).contains("Missing 'flow_id'"));
    let no_graph = tool.execute(json!({ "flow_id": "f" })).await.unwrap();
    assert!(text(&no_graph).contains("Provide `draft_id`"));

    h.put_flow("flow-1", "Digest", false, valid_graph());
    *h.gate_errors.lock().unwrap() = vec!["unwired arg".to_string()];
    let gated = tool
        .execute(json!({ "flow_id": "flow-1", "graph": valid_graph() }))
        .await
        .unwrap();
    assert!(gated.is_error);
    assert!(text(&gated).contains("call save_workflow again"));
}

// ── run control and reads ────────────────────────────────────────────────────

#[tokio::test]
async fn cancel_refuses_a_run_that_belongs_to_another_flow() {
    let h = host();
    h.put_run("run-1", "flow-a", "pending_approval");
    let tool = CancelFlowRunTool::new(h.clone());
    let refused = tool
        .execute(json!({ "flow_id": "flow-b", "run_id": "run-1" }))
        .await
        .unwrap();
    assert!(refused.is_error);
    assert!(text(&refused).contains("belongs to flow 'flow-a', not 'flow-b'"));
    assert!(h.calls.lock().unwrap().is_empty(), "nothing was cancelled");

    let ok = tool
        .execute(json!({ "flow_id": "flow-a", "run_id": "run-1" }))
        .await
        .unwrap();
    assert!(!ok.is_error);
    assert_eq!(h.calls.lock().unwrap().as_slice(), ["cancel run-1"]);
}

#[tokio::test]
async fn resume_forwards_approvals_and_ignores_non_strings() {
    let h = host();
    let tool = ResumeFlowRunTool::new(h.clone());
    let result = tool
        .execute(
            json!({ "flow_id": "f", "run_id": "r", "approve": ["n1", 7, "n2"],
                         "reject": ["n3"] }),
        )
        .await
        .unwrap();
    assert!(!result.is_error);
    assert_eq!(h.calls.lock().unwrap().as_slice(), ["resume f r +2 -1"]);
    let missing = tool.execute(json!({ "flow_id": "f" })).await.unwrap();
    assert!(text(&missing).contains("Missing 'run_id'"));
}

#[tokio::test]
async fn flow_reads_shape_their_payloads() {
    let h = host();
    h.put_flow("flow-1", "Digest", true, valid_graph());
    h.put_run("run-1", "flow-1", "completed");

    let listed = json_of(
        &ListFlowsTool::new(h.clone())
            .execute(json!({}))
            .await
            .unwrap(),
    );
    assert_eq!(listed["flows"][0]["id"], "flow-1");
    assert_eq!(listed["flows"][0]["enabled"], true);

    let one = json_of(
        &GetFlowTool::new(h.clone())
            .execute(json!({ "id": "flow-1" }))
            .await
            .unwrap(),
    );
    assert_eq!(one["graph"]["nodes"][0]["id"], "t");

    let runs = json_of(
        &ListFlowRunsTool::new(h.clone())
            .execute(json!({ "flow_id": "flow-1" }))
            .await
            .unwrap(),
    );
    assert_eq!(runs["runs"][0]["id"], "run-1");

    let run = json_of(
        &GetFlowRunTool::new(h.clone())
            .execute(json!({ "run_id": "run-1" }))
            .await
            .unwrap(),
    );
    assert_eq!(run["status"], "completed");

    let history = json_of(
        &GetFlowHistoryTool::new(h.clone())
            .execute(json!({ "flow_id": "flow-1" }))
            .await
            .unwrap(),
    );
    assert_eq!(history["revisions"], json!([]));

    let gone = GetFlowTool::new(h)
        .execute(json!({ "id": "ghost" }))
        .await
        .unwrap();
    assert!(text(&gone).contains("Failed to get flow 'ghost'"));
}

// ── registry reads ───────────────────────────────────────────────────────────

#[tokio::test]
async fn node_kind_tools_read_the_host_supplied_catalog() {
    let kinds: Arc<dyn NodeKindSource> = Arc::new(OverlayKinds);
    let listed = json_of(
        &ListNodeKindsTool::new(kinds.clone())
            .execute(json!({}))
            .await
            .unwrap(),
    );
    assert_eq!(
        listed["node_kinds"].as_array().unwrap().len(),
        tinyflows::catalog::NODE_KINDS.len()
    );

    let contract = json_of(
        &GetNodeKindContractTool::new(kinds.clone())
            .execute(json!({ "kind": "tool_call" }))
            .await
            .unwrap(),
    );
    assert!(
        contract.to_string().contains("overlay"),
        "host overlay reached the model"
    );

    let unknown = GetNodeKindContractTool::new(kinds)
        .execute(json!({ "kind": "banana" }))
        .await
        .unwrap();
    assert!(unknown.is_error);
    assert!(text(&unknown).contains("'banana' is not a tinyflows node kind"));
}

#[tokio::test]
async fn agent_definitions_are_rendered_from_the_directory() {
    let h = host();
    let payload = json_of(
        &ListAgentDefinitionsTool::new(h)
            .execute(json!({}))
            .await
            .unwrap(),
    );
    let first = &payload["agent_definitions"][0];
    assert_eq!(first["id"], "agent_memory");
    assert_eq!(first["model"], Value::Null);
    assert_eq!(first["tools"], json!(["memory_recall"]));
}

// ── discovery ────────────────────────────────────────────────────────────────

fn suggestion(title: &str) -> Value {
    json!({
        "title": title,
        "one_liner": "Does a thing.",
        "rationale": "You do it weekly.",
        "build_prompt": "Build it.",
        "confidence": 3.0,
        "steps_outline": ["a", "  ", "b"]
    })
}

#[tokio::test]
async fn suggestions_are_deduplicated_by_title_and_persisted() {
    let h = host();
    let tool = SuggestWorkflowsTool::new(h.clone());
    let result = tool
        .execute(json!({
            "run_id": "run-9",
            "suggestions": [ suggestion("Auto-file  receipts"), suggestion("auto-file receipts"),
                             suggestion("Other") ]
        }))
        .await
        .unwrap();
    assert!(!result.is_error, "{}", text(&result));
    let payload = json_of(&result);
    assert_eq!(payload["count"], 2);
    let stored = h.suggestions.lock().unwrap();
    assert_eq!(stored.len(), 2);
    assert!(stored[0].id.starts_with("sug_") && stored[0].id.len() == 4 + 16);
    assert_eq!(stored[0].confidence, 1.0, "confidence is clamped");
    assert_eq!(stored[0].steps_outline, ["a", "b"]);
    assert_eq!(stored[0].source_run_id.as_deref(), Some("run-9"));
}

#[tokio::test]
async fn suggestion_ids_are_stable_across_whitespace_and_case() {
    let h = host();
    let tool = SuggestWorkflowsTool::new(h.clone());
    tool.execute(json!({ "suggestions": [ suggestion("Auto-file receipts") ] }))
        .await
        .unwrap();
    tool.execute(json!({ "suggestions": [ suggestion("  AUTO-FILE   receipts ") ] }))
        .await
        .unwrap();
    let stored = h.suggestions.lock().unwrap();
    assert_eq!(stored[0].id, stored[1].id);
}

#[tokio::test]
async fn suggestions_missing_a_field_are_rejected_by_name() {
    let tool = SuggestWorkflowsTool::new(host());
    let mut bad = suggestion("X");
    bad.as_object_mut().unwrap().remove("rationale");
    let result = tool.execute(json!({ "suggestions": [bad] })).await.unwrap();
    assert!(result.is_error);
    assert!(text(&result).contains("suggestion #1: missing 'rationale'"));
    let empty = tool.execute(json!({ "suggestions": [] })).await.unwrap();
    assert!(text(&empty).contains("Missing or empty 'suggestions'"));
}
