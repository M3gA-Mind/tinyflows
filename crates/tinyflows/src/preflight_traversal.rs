//! Saved-workflow discovery and bounded mock preflight traversal.

use super::{MockInputs, RUN_TIMEOUT_SECS, graph_unresolvable_tool_args};
use crate::model::{NodeKind, WorkflowGraph};
use serde_json::Value;
use std::sync::Arc;

pub(super) async fn check(
    graph: &WorkflowGraph,
    native_slug_prefixes: &[&str],
    host: Option<&dyn crate::caps::WorkflowResolver>,
) -> Vec<String> {
    let max_depth = crate::compat::max_sub_workflow_depth(graph);
    let completed = std::sync::Mutex::new(Vec::new());
    let check = async {
        let mut saved: std::collections::HashMap<String, WorkflowGraph> =
            std::collections::HashMap::new();
        let mut shallowest = std::collections::HashMap::new();
        let mut lookup = vec![(graph.clone(), 0_u64)];
        while let Some((current, depth)) = lookup.pop() {
            if depth >= max_depth {
                continue;
            }
            for node in &current.nodes {
                if node.kind != NodeKind::SubWorkflow {
                    continue;
                }
                if let Some(inline) = node.config.get("workflow") {
                    if let Ok(child) = serde_json::from_value::<WorkflowGraph>(inline.clone()) {
                        lookup.push((child, depth + 1));
                    }
                } else if let (Some(host), Some(id)) = (
                    host,
                    node.config
                        .get("workflow_id")
                        .and_then(Value::as_str)
                        .filter(|id| !crate::expr::is_expression(id)),
                ) {
                    let child_depth = depth + 1;
                    if shallowest.get(id).is_none_or(|seen| child_depth < *seen) {
                        let child = if let Some(child) = saved.get(id) {
                            Some(child.clone())
                        } else {
                            host.resolve(id).await.ok()
                        };
                        if let Some(child) = child {
                            saved.insert(id.to_owned(), child.clone());
                            shallowest.insert(id.to_owned(), child_depth);
                            lookup.push((child, child_depth));
                        }
                    }
                }
            }
        }
        let mut resolver = crate::caps::mock::MockWorkflowResolver::default();
        for (id, child) in &saved {
            resolver = resolver.with(id, child.clone());
        }
        let resolver: Arc<dyn crate::caps::WorkflowResolver> = Arc::new(resolver);
        let mut pending = vec![(graph.clone(), 0_u64, String::new(), MockInputs::root(graph))];
        while let Some((current, depth, prefix, inputs)) = pending.pop() {
            let findings = graph_unresolvable_tool_args(
                &current,
                native_slug_prefixes,
                inputs.clone(),
                resolver.clone(),
            )
            .await
            .into_iter()
            .map(|message| format!("{prefix}{message}"));
            completed.lock().expect("completed checks").extend(findings);
            if depth >= max_depth {
                continue;
            }
            for node in current.nodes.iter().rev() {
                if node.kind != NodeKind::SubWorkflow {
                    continue;
                }
                let child = node
                    .config
                    .get("workflow")
                    .and_then(|inline| serde_json::from_value::<WorkflowGraph>(inline.clone()).ok())
                    .or_else(|| {
                        node.config
                            .get("workflow_id")
                            .and_then(Value::as_str)
                            .and_then(|id| saved.get(id))
                            .cloned()
                    });
                if let Some(child) = child {
                    let child_inputs = inputs.child(&child, &node.config);
                    pending.push((
                        child,
                        depth + 1,
                        format!("{prefix}Inline sub_workflow node '{}': ", node.id),
                        child_inputs,
                    ));
                }
            }
        }
    };
    within_deadline(check, &completed).await
}

/// Bound unfinished work while retaining diagnostics from settled mock runs.
pub(super) async fn within_deadline(
    check: impl std::future::Future<Output = ()>,
    completed: &std::sync::Mutex<Vec<String>>,
) -> Vec<String> {
    if tokio::time::timeout(std::time::Duration::from_secs(RUN_TIMEOUT_SECS), check)
        .await
        .is_err()
    {
        Vec::new()
    } else {
        std::mem::take(&mut *completed.lock().expect("completed checks"))
    }
}
