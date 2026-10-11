//! Compatibility of *saved* workflows a graph references by id.
//!
//! [`errors`](super::errors) walks inline children. A `sub_workflow` that names a
//! saved workflow (`workflow_id`) needs the host's store to look the child up, so
//! the walk takes a resolver closure and the host keeps the store.

use std::collections::HashMap;

use serde_json::Value;

use super::{errors_with_max_depth, max_sub_workflow_depth};
use crate::model::{NodeKind, WorkflowGraph};

/// The first engine-incompatible topology reachable through literal
/// `workflow_id` children of `graph`, as a one-element list (empty when none).
///
/// `resolve` maps a saved workflow id to its graph, or `None` when it is
/// missing or cannot be loaded; such references are skipped and keep their
/// runtime diagnostics. Dynamic `=` expressions and ids sharing a node with an
/// inline `workflow` are skipped too: this only rejects a saved graph whose
/// topology is demonstrably unsafe.
///
/// The walk descends as deep as the root declares it may nest
/// ([`max_sub_workflow_depth`]), and checks each saved child to the *remaining*
/// depth budget, because the engine's runtime depth counter is one budget shared
/// across the whole inline-plus-referenced chain.
pub fn referenced_workflow_errors(
    graph: &WorkflowGraph,
    resolve: &dyn Fn(&str) -> Option<WorkflowGraph>,
) -> Vec<String> {
    let mut walk = ReferenceWalk::new(graph);
    while let Some(reference) = walk.next() {
        let Some(child) = resolve(&reference.id) else {
            continue;
        };
        if let Some(error) = walk.resolved(reference, child) {
            return vec![error];
        }
    }
    Vec::new()
}

/// Asynchronous saved-workflow compatibility using the engine's host resolver.
///
/// Lookup failures are skipped, as in [`referenced_workflow_errors`], leaving
/// missing/unavailable references to runtime diagnostics. The shared walker
/// preserves synchronous diagnostic precedence, lookup order, cycle handling,
/// and the root's remaining depth budget without blocking an async executor.
pub async fn referenced_workflow_errors_async(
    graph: &WorkflowGraph,
    resolve: &dyn crate::caps::WorkflowResolver,
) -> Vec<String> {
    let mut walk = ReferenceWalk::new(graph);
    while let Some(reference) = walk.next() {
        let Ok(child) = resolve.resolve(&reference.id).await else {
            continue;
        };
        if let Some(error) = walk.resolved(reference, child) {
            return vec![error];
        }
    }
    Vec::new()
}

struct Frame {
    graph: WorkflowGraph,
    depth: u64,
    path: Vec<String>,
    next_node: usize,
}

struct Reference {
    id: String,
    depth: u64,
    path: Vec<String>,
}

/// Suspend at each host lookup, retaining the old per-frame node order before
/// descending into children. Both resolver APIs use exactly the same state.
struct ReferenceWalk {
    max_depth: u64,
    pending: Vec<Frame>,
    current: Option<Frame>,
    // A deep visit must not suppress a later shallower visit with more budget.
    visited_depths: HashMap<String, u64>,
}

impl ReferenceWalk {
    fn new(graph: &WorkflowGraph) -> Self {
        Self {
            max_depth: max_sub_workflow_depth(graph),
            pending: vec![Frame {
                graph: graph.clone(),
                depth: 0,
                path: Vec::new(),
                next_node: 0,
            }],
            current: None,
            visited_depths: HashMap::new(),
        }
    }

    fn next(&mut self) -> Option<Reference> {
        loop {
            if self.current.is_none() {
                self.current = Some(self.pending.pop()?);
            }
            let current = self.current.as_mut()?;
            if current.depth >= self.max_depth || current.next_node >= current.graph.nodes.len() {
                self.current = None;
                continue;
            }
            let node = &current.graph.nodes[current.next_node];
            current.next_node += 1;
            if node.kind != NodeKind::SubWorkflow {
                continue;
            }
            let mut path = current.path.clone();
            path.push(node.id.clone());
            let depth = current.depth + 1;
            let inline = node.config.get("workflow");
            let id = node
                .config
                .get("workflow_id")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|id| !id.is_empty());
            // Invalid dual-source nodes retain structural-validation precedence.
            if inline.is_some() && id.is_some() {
                continue;
            }
            if let Some(inline) = inline {
                if let Ok(graph) = serde_json::from_value::<WorkflowGraph>(inline.clone()) {
                    self.pending.push(Frame {
                        graph,
                        depth,
                        path,
                        next_node: 0,
                    });
                }
                continue;
            }
            let Some(id) = id.filter(|id| !id.starts_with('=')) else {
                continue;
            };
            if self
                .visited_depths
                .get(id)
                .is_some_and(|seen| *seen <= depth)
            {
                continue;
            }
            self.visited_depths.insert(id.to_owned(), depth);
            return Some(Reference {
                id: id.to_owned(),
                depth,
                path,
            });
        }
    }

    fn resolved(&mut self, reference: Reference, graph: WorkflowGraph) -> Option<String> {
        let remaining_depth = self.max_depth.saturating_sub(reference.depth);
        if let Some(error) = errors_with_max_depth(&graph, remaining_depth)
            .into_iter()
            .next()
        {
            return Some(format!(
                "Sub_workflow path '{}' references workflow_id '{}' with an unsupported \
                 engine topology: {}: {}",
                reference.path.join(" -> "),
                reference.id,
                error.code,
                error.message
            ));
        }
        self.pending.push(Frame {
            graph,
            depth: reference.depth,
            path: reference.path,
            next_node: 0,
        });
        None
    }
}

#[cfg(test)]
#[path = "referenced_tests.rs"]
mod tests;
