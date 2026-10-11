//! Conservative propagation of unavailable caller inputs through node outputs.

use std::collections::HashSet;

use crate::{bindings, model::WorkflowGraph};

use super::{explicit_nodes_ref, input_scope::MockInputs};

/// Capability-backed nodes may derive their entire output from config inputs.
/// Keep those outputs opaque, like tool-call echoes, until real data is available.
pub(super) fn unknown_nodes(graph: &WorkflowGraph, inputs: &MockInputs) -> HashSet<String> {
    let mut unknown = HashSet::new();
    loop {
        let before = unknown.len();
        for node in &graph.nodes {
            if bindings::collect_expressions(&node.config)
                .iter()
                .any(|(_, expression)| {
                    is_unverifiable(expression, graph, &node.id, inputs, &unknown)
                })
            {
                unknown.insert(node.id.clone());
            }
        }
        if unknown.len() == before {
            return unknown;
        }
    }
}

/// A downstream expression cannot turn an input-derived mock value into proof.
pub(super) fn is_unverifiable(
    expression: &str,
    graph: &WorkflowGraph,
    node_id: &str,
    inputs: &MockInputs,
    unknown: &HashSet<String>,
) -> bool {
    if inputs.is_unverifiable(expression) {
        return true;
    }
    let body = expression.strip_prefix('=').unwrap_or(expression).trim();
    let body = body.strip_prefix('.').unwrap_or(body);
    // Rooted input paths retain field-level precision. Compound jq programs
    // may use inputs anywhere; interpreting their output shape needs real data.
    if !body.starts_with("inputs")
        && [".inputs.", ".inputs[", ".inputs "]
            .iter()
            .any(|reference| body.contains(reference))
        && inputs.is_unverifiable("=inputs")
    {
        return true;
    }
    if let Some(id) = explicit_nodes_ref(expression)
        && unknown.contains(id)
    {
        return true;
    }
    for id in unknown {
        if [
            format!(".nodes.{id}."),
            format!(".nodes[\"{id}\"]"),
            format!(".nodes['{id}']"),
        ]
        .iter()
        .any(|reference| body.contains(reference))
        {
            return true;
        }
    }
    let item_scoped = ["item.", "item[", "items.", "items["]
        .iter()
        .any(|prefix| body.starts_with(prefix))
        || body == "item"
        || body == "items"
        || [".item.", ".item[", ".items.", ".items["]
            .iter()
            .any(|reference| body.contains(reference));
    item_scoped
        && graph
            .edges
            .iter()
            .any(|edge| edge.to_node == node_id && unknown.contains(&edge.from_node))
}
