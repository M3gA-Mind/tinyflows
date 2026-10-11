//! Conservative propagation of unavailable caller inputs through node outputs.

use std::collections::HashMap;

use crate::{
    bindings,
    model::{NodeKind, WorkflowGraph},
};

use super::input_scope::{MockInputs, scope_path};

type UnknownOutputs = HashMap<String, Vec<Vec<String>>>;

/// Track transform fields independently; capability outputs with unknown config are opaque.
pub(super) fn unknown_nodes(graph: &WorkflowGraph, inputs: &MockInputs) -> UnknownOutputs {
    let mut unknown: UnknownOutputs = HashMap::new();
    loop {
        let mut changed = false;
        for node in &graph.nodes {
            let set = (node.kind == NodeKind::Transform)
                .then(|| {
                    node.config
                        .get("set")
                        .and_then(serde_json::Value::as_object)
                })
                .flatten();
            let mut paths = Vec::new();
            if node.kind == NodeKind::Transform {
                // Transform overlays `set` onto its input, preserving other fields.
                for edge in graph.edges.iter().filter(|edge| edge.to_node == node.id) {
                    for path in unknown.get(&edge.from_node).into_iter().flatten() {
                        if !path
                            .first()
                            .is_some_and(|field| set.is_some_and(|set| set.contains_key(field)))
                        {
                            paths.push(path.clone());
                        }
                    }
                }
            }
            for (location, expression) in bindings::collect_expressions(&node.config) {
                if is_unverifiable(&expression, graph, &node.id, inputs, &unknown) {
                    let path = if node.kind == NodeKind::Transform {
                        location
                            .strip_prefix("set.")
                            .map(|field| vec![field.split('.').next().unwrap_or(field).to_string()])
                            .unwrap_or_default()
                    } else {
                        Vec::new()
                    };
                    paths.push(path);
                }
            }
            let existing = unknown.entry(node.id.clone()).or_default();
            for path in paths {
                if !existing.contains(&path) {
                    existing.push(path);
                    changed = true;
                }
            }
        }
        if !changed {
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
    unknown: &UnknownOutputs,
) -> bool {
    if inputs.is_unverifiable(expression) {
        return true;
    }
    let body = expression.strip_prefix('=').unwrap_or(expression).trim();
    let body = body.strip_prefix('.').unwrap_or(body);
    if !body.starts_with("inputs")
        && [".inputs.", ".inputs[", ".inputs "]
            .iter()
            .any(|reference| body.contains(reference))
        && inputs.is_unverifiable("=inputs")
    {
        return true;
    }
    if let Some(path) = scope_path(expression, "nodes") {
        if let Some(id) = path.first() {
            // The node scope wraps each node's first output under `item`.
            return path
                .get(1)
                .is_none_or(|segment| segment != "item")
                .then(|| unknown.get(id).is_some_and(|paths| !paths.is_empty()))
                .unwrap_or_else(|| intersects(unknown.get(id), &path[2..]));
        }
        return unknown.values().any(|paths| !paths.is_empty());
    }
    for id in unknown.keys() {
        if [
            format!(".nodes.{id}."),
            format!(".nodes[\"{id}\"]"),
            format!(".nodes['{id}']"),
        ]
        .iter()
        .any(|reference| body.contains(reference))
            && unknown.get(id).is_some_and(|paths| !paths.is_empty())
        {
            return true;
        }
    }
    let path = scope_path(expression, "item").or_else(|| {
        scope_path(expression, "items").map(|mut path| {
            if path
                .first()
                .is_some_and(|index| index.chars().all(|c| c.is_ascii_digit()))
            {
                path.remove(0);
            }
            path
        })
    });
    if let Some(path) = path {
        return graph.edges.iter().any(|edge| {
            edge.to_node == node_id && intersects(unknown.get(&edge.from_node), &path)
        });
    }
    let item_scoped = [".item.", ".item[", ".items.", ".items["]
        .iter()
        .any(|reference| body.contains(reference));
    item_scoped
        && graph.edges.iter().any(|edge| {
            edge.to_node == node_id
                && unknown
                    .get(&edge.from_node)
                    .is_some_and(|paths| !paths.is_empty())
        })
}

fn intersects(paths: Option<&Vec<Vec<String>>>, read: &[String]) -> bool {
    paths
        .into_iter()
        .flatten()
        .any(|path| read.starts_with(path) || path.starts_with(read))
}
