//! In-memory implementations of every port, for the tool tests.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use serde_json::{Value, json};
use tinyflows::catalog::NodeKindContract;
use tinyflows::model::WorkflowGraph;
use tinyflows_catalog::types::FlowSuggestion;
use tinyflows_catalog::{DraftOrigin, Flow, FlowDraft, FlowRevision, FlowRun, FlowValidation};

use super::ports::*;

/// One struct that plays every port, with the knobs the tests turn.
#[derive(Default)]
pub struct MemHost {
    pub drafts: Mutex<HashMap<String, FlowDraft>>,
    pub flows: Mutex<HashMap<String, Flow>>,
    pub runs: Mutex<HashMap<String, FlowRun>>,
    pub suggestions: Mutex<Vec<FlowSuggestion>>,
    /// Gate errors `run_gates` reports.
    pub gate_errors: Mutex<Vec<String>>,
    /// Makes `DraftStore::update` fail.
    pub fail_draft_update: Mutex<bool>,
    /// Makes `FlowStore::set_enabled` fail.
    pub fail_set_enabled: Mutex<bool>,
    /// Calls the cancel/resume ports received, for assertions.
    pub calls: Mutex<Vec<String>>,
    next_id: Mutex<u32>,
}

impl MemHost {
    fn next(&self, prefix: &str) -> String {
        let mut n = self.next_id.lock().unwrap();
        *n += 1;
        format!("{prefix}-{n}")
    }

    pub fn put_flow(&self, id: &str, name: &str, enabled: bool, graph: Value) {
        let graph = tinyflows::migrate::deserialize_graph(graph).expect("fixture graph");
        self.flows.lock().unwrap().insert(
            id.to_string(),
            Flow {
                id: id.to_string(),
                name: name.to_string(),
                enabled,
                graph,
                created_at: "t0".into(),
                updated_at: "t0".into(),
                last_run_at: None,
                last_status: None,
                require_approval: true,
            },
        );
    }

    pub fn put_draft(&self, id: &str, flow_id: Option<&str>, name: &str, graph: Value) {
        self.drafts.lock().unwrap().insert(
            id.to_string(),
            FlowDraft {
                id: id.to_string(),
                flow_id: flow_id.map(str::to_string),
                name: name.to_string(),
                graph,
                origin: DraftOrigin::Chat,
                created_at: "t0".into(),
                updated_at: "t0".into(),
            },
        );
    }

    pub fn put_run(&self, id: &str, flow_id: &str, status: &str) {
        let run: FlowRun = serde_json::from_value(json!({
            "id": id, "flow_id": flow_id, "thread_id": id, "status": status,
            "started_at": "t0", "finished_at": null,
        }))
        .expect("fixture run");
        self.runs.lock().unwrap().insert(id.to_string(), run);
    }
}

#[async_trait]
impl DraftStore for MemHost {
    async fn get(&self, id: &str) -> PortResult<FlowDraft> {
        self.drafts
            .lock()
            .unwrap()
            .get(id)
            .cloned()
            .ok_or_else(|| format!("draft '{id}' not found"))
    }

    async fn create(
        &self,
        flow_id: Option<String>,
        name: String,
        graph: Value,
        origin: DraftOrigin,
    ) -> PortResult<FlowDraft> {
        let id = self.next("draft");
        let draft = FlowDraft {
            id: id.clone(),
            flow_id,
            name,
            graph,
            origin,
            created_at: "t0".into(),
            updated_at: "t0".into(),
        };
        self.drafts.lock().unwrap().insert(id, draft.clone());
        Ok(draft)
    }

    async fn update(
        &self,
        id: &str,
        name: Option<String>,
        graph: Option<Value>,
    ) -> PortResult<FlowDraft> {
        if *self.fail_draft_update.lock().unwrap() {
            return Err("disk full".to_string());
        }
        let mut drafts = self.drafts.lock().unwrap();
        let draft = drafts
            .get_mut(id)
            .ok_or_else(|| format!("draft '{id}' not found"))?;
        if let Some(name) = name {
            draft.name = name;
        }
        if let Some(graph) = graph {
            draft.graph = graph;
        }
        Ok(draft.clone())
    }
}

#[async_trait]
impl FlowStore for MemHost {
    async fn get(&self, id: &str) -> PortResult<Flow> {
        self.flows
            .lock()
            .unwrap()
            .get(id)
            .cloned()
            .ok_or_else(|| format!("flow '{id}' not found"))
    }

    async fn list(&self) -> PortResult<Vec<Flow>> {
        let mut flows: Vec<Flow> = self.flows.lock().unwrap().values().cloned().collect();
        flows.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(flows)
    }

    async fn load_graph(&self, id: &str) -> PortResult<Option<WorkflowGraph>> {
        Ok(self.flows.lock().unwrap().get(id).map(|f| f.graph.clone()))
    }

    async fn create(&self, name: String, graph: Value, require_approval: bool) -> PortResult<Flow> {
        let id = self.next("flow");
        let parsed = tinyflows::migrate::deserialize_graph(graph)?;
        // Mirrors the host's Rule 1: a manual-trigger graph is born enabled.
        let enabled = !tinyflows_catalog::graph_policy::trigger_is_automatic(&parsed);
        let flow = Flow {
            id: id.clone(),
            name,
            enabled,
            graph: parsed,
            created_at: "t0".into(),
            updated_at: "t0".into(),
            last_run_at: None,
            last_status: None,
            require_approval,
        };
        self.flows.lock().unwrap().insert(id, flow.clone());
        Ok(flow)
    }

    async fn update(
        &self,
        id: &str,
        name: Option<String>,
        graph: Option<Value>,
    ) -> PortResult<Written<Flow>> {
        let mut flows = self.flows.lock().unwrap();
        let flow = flows
            .get_mut(id)
            .ok_or_else(|| format!("flow '{id}' not found"))?;
        if let Some(name) = name {
            flow.name = name;
        }
        if let Some(graph) = graph {
            flow.graph = tinyflows::migrate::deserialize_graph(graph)?;
        }
        Ok(Written {
            value: flow.clone(),
            logs: vec![format!("flow updated: {id}"), "note from host".to_string()],
        })
    }

    async fn set_enabled(&self, id: &str, enabled: bool) -> PortResult<Flow> {
        if *self.fail_set_enabled.lock().unwrap() {
            return Err("store busy".to_string());
        }
        let mut flows = self.flows.lock().unwrap();
        let flow = flows
            .get_mut(id)
            .ok_or_else(|| format!("flow '{id}' not found"))?;
        flow.enabled = enabled;
        Ok(flow.clone())
    }

    async fn duplicate(&self, id: &str) -> PortResult<Flow> {
        let source = FlowStore::get(self, id).await?;
        let copy_id = self.next("flow");
        let copy = Flow {
            id: copy_id.clone(),
            name: format!("{} (copy)", source.name),
            enabled: false,
            ..source
        };
        self.flows.lock().unwrap().insert(copy_id, copy.clone());
        Ok(copy)
    }

    async fn history(&self, id: &str, limit: usize) -> PortResult<Vec<FlowRevision>> {
        let _ = (id, limit);
        Ok(Vec::new())
    }

    async fn list_runs(&self, flow_id: &str, limit: usize) -> PortResult<Vec<FlowRun>> {
        Ok(self
            .runs
            .lock()
            .unwrap()
            .values()
            .filter(|r| r.flow_id == flow_id)
            .take(limit)
            .cloned()
            .collect())
    }

    async fn get_run(&self, run_id: &str) -> PortResult<FlowRun> {
        self.runs
            .lock()
            .unwrap()
            .get(run_id)
            .cloned()
            .ok_or_else(|| format!("flow run '{run_id}' not found"))
    }

    async fn resume(
        &self,
        flow_id: &str,
        run_id: &str,
        approve: Vec<String>,
        reject: Vec<String>,
    ) -> PortResult<Value> {
        self.calls.lock().unwrap().push(format!(
            "resume {flow_id} {run_id} +{} -{}",
            approve.len(),
            reject.len()
        ));
        Ok(json!({ "resumed": run_id }))
    }

    async fn cancel_run(&self, run_id: &str) -> PortResult<Value> {
        self.calls.lock().unwrap().push(format!("cancel {run_id}"));
        Ok(json!({ "cancelled": run_id }))
    }
}

#[async_trait]
impl BuilderGates for MemHost {
    async fn validate(&self, graph: Value) -> FlowValidation {
        match tinyflows::migrate::deserialize_graph(graph) {
            Err(e) => FlowValidation {
                valid: false,
                errors: vec![e],
                ..FlowValidation::default()
            },
            Ok(graph) => {
                let errors: Vec<String> = tinyflows::validate::validate_all(&graph)
                    .iter()
                    .map(ToString::to_string)
                    .collect();
                FlowValidation {
                    valid: errors.is_empty(),
                    errors,
                    ..FlowValidation::default()
                }
            }
        }
    }

    async fn run_gates(&self, _graph: &WorkflowGraph) -> Vec<String> {
        self.gate_errors.lock().unwrap().clone()
    }

    async fn strict_gate(&self, graph: &Value) -> PortResult<()> {
        let validation = self.validate(graph.clone()).await;
        if !validation.valid {
            return Err(format!(
                "strict validation failed:\n{}",
                validation.errors.join("\n")
            ));
        }
        let gates = self.gate_errors.lock().unwrap().clone();
        if gates.is_empty() {
            Ok(())
        } else {
            Err(format!("strict validation failed:\n{}", gates.join("\n\n")))
        }
    }

    async fn engine_compatibility_errors(&self, _graph: &WorkflowGraph) -> Vec<String> {
        Vec::new()
    }

    async fn wiring_warnings(&self, _graph: &WorkflowGraph) -> Vec<String> {
        Vec::new()
    }

    async fn approval_manifest(&self, _graph: &WorkflowGraph) -> Vec<Value> {
        Vec::new()
    }

    async fn build_proposal(
        &self,
        retry_tool: &str,
        name: &str,
        graph: &WorkflowGraph,
        require_approval: bool,
        revision: bool,
        instruction: Option<String>,
        draft_id: Option<String>,
        flow_id: Option<String>,
    ) -> Result<Value, String> {
        let gates = self.gate_errors.lock().unwrap().clone();
        if !gates.is_empty() {
            return Err(format!(
                "{}\n\nFix these and call {retry_tool} again.",
                gates.join("\n\n")
            ));
        }
        Ok(json!({
            "type": "workflow_proposal",
            "name": name,
            "node_count": graph.nodes.len(),
            "require_approval": require_approval,
            "revision": revision,
            "instruction": instruction,
            "draft_id": draft_id,
            "flow_id": flow_id,
            "persisted": false,
        }))
    }

    fn sandbox_tool_invoker(&self) -> std::sync::Arc<dyn tinyflows::caps::ToolInvoker> {
        tinyflows::caps::mock::mock_capabilities().tools
    }
}

#[async_trait]
impl AgentDirectory for MemHost {
    async fn list(&self) -> PortResult<Vec<AgentDefinition>> {
        Ok(vec![AgentDefinition {
            id: "agent_memory".into(),
            name: "Memory".into(),
            description: "Looks things up".into(),
            model: Value::Null,
            tools: json!(["memory_recall"]),
            tags: json!(["memory"]),
        }])
    }
}

#[async_trait]
impl SuggestionStore for MemHost {
    async fn upsert(&self, suggestions: &[FlowSuggestion]) -> PortResult<usize> {
        self.suggestions
            .lock()
            .unwrap()
            .extend_from_slice(suggestions);
        Ok(suggestions.len())
    }
}

/// A node-kind source with one marker note per kind, to show the host overlay is
/// what the tools read.
pub struct OverlayKinds;

impl NodeKindSource for OverlayKinds {
    fn all(&self) -> Vec<NodeKindContract> {
        tinyflows::catalog::all_contracts()
            .into_iter()
            .map(|c| c.with_note("overlay"))
            .collect()
    }

    fn get(&self, kind: &str) -> Option<NodeKindContract> {
        tinyflows::catalog::contract_for(kind).map(|c| c.with_note("overlay"))
    }
}
