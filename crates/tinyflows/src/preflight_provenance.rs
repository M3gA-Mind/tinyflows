//! Conservative field provenance for outputs derived from unavailable caller data.

use super::input_scope::{MockInputs, scope_reads};
use crate::model::{NodeKind, WorkflowGraph};
use serde_json::Value;
use std::collections::HashMap;

type Paths = Vec<Vec<String>>;

pub(super) struct Provenance<'a> {
    graph: &'a WorkflowGraph,
    inputs: &'a MockInputs,
    outputs: HashMap<String, Paths>,
}

impl<'a> Provenance<'a> {
    pub(super) fn new(graph: &'a WorkflowGraph, inputs: &'a MockInputs) -> Self {
        let mut result = Self {
            graph,
            inputs,
            outputs: HashMap::new(),
        };
        // A monotone fixed point handles graph order and dependent chains.
        for _ in 0..=graph.nodes.len() {
            let mut changed = false;
            for node in &graph.nodes {
                let mut paths = Vec::new();
                if node.kind == NodeKind::Transform {
                    let set = node.config.get("set").and_then(Value::as_object);
                    for edge in graph.edges.iter().filter(|edge| edge.to_node == node.id) {
                        for path in result.outputs.get(&edge.from_node).into_iter().flatten() {
                            if !path
                                .first()
                                .is_some_and(|field| set.is_some_and(|set| set.contains_key(field)))
                            {
                                paths.push(path.clone());
                            }
                        }
                    }
                    for key in ["set", "template"] {
                        if let Some(Value::Object(fields)) = node.config.get(key) {
                            for (field, value) in fields {
                                if result.value_is_unknown(value, &node.id) {
                                    paths.push(vec![field.clone()]);
                                }
                            }
                        }
                    }
                    // Non-field transform expressions can change the whole output.
                    if node.config.as_object().is_some_and(|config| {
                        config.iter().any(|(key, value)| {
                            key != "set"
                                && key != "template"
                                && result.value_is_unknown(value, &node.id)
                        })
                    }) {
                        paths.push(Vec::new());
                    }
                } else if result.value_is_unknown(&node.config, &node.id) {
                    paths.push(Vec::new());
                }
                let entry = result.outputs.entry(node.id.clone()).or_default();
                for path in paths {
                    if !entry.contains(&path) {
                        entry.push(path);
                        changed = true;
                    }
                }
            }
            if !changed {
                break;
            }
        }
        result
    }

    fn value_is_unknown(&self, value: &Value, node: &str) -> bool {
        match value {
            Value::String(expression) => self.is_unverifiable(expression, node),
            Value::Object(values) => values
                .values()
                .any(|value| self.value_is_unknown(value, node)),
            Value::Array(values) => values
                .iter()
                .any(|value| self.value_is_unknown(value, node)),
            _ => false,
        }
    }

    pub(super) fn is_unverifiable(&self, expression: &str, node: &str) -> bool {
        let trigger = self
            .graph
            .nodes
            .iter()
            .find(|node| node.kind == NodeKind::Trigger)
            .map(|node| node.id.as_str());
        if super::is_trigger_scoped_expression(expression, self.graph, node, trigger) {
            return true;
        }
        scope_reads(expression)
            .iter()
            .any(|(scope, path)| match scope.as_str() {
                "inputs" => intersects(&self.inputs.unknown, path),
                "run" => true,
                "nodes" => {
                    let Some((id, rest)) = path.split_first() else {
                        return self.outputs.values().any(|paths| !paths.is_empty())
                            || self.graph.nodes.iter().any(|node| trigger_passthrough(self.graph, &node.id, &[], self.graph.nodes.len()));
                    };
                    let rest = output_path(rest);
                    self.outputs
                        .get(id)
                        .is_some_and(|paths| intersects(paths, rest))
                        || trigger_passthrough(self.graph, id, rest, self.graph.nodes.len())
                }
                "item" | "items" => self
                    .graph
                    .edges
                    .iter()
                    .filter(|edge| edge.to_node == node)
                    .any(|edge| {
                        let path = if scope == "items"
                            && path.first().is_some_and(|key| key.parse::<usize>().is_ok())
                        {
                            &path[1..]
                        } else {
                            path.as_slice()
                        };
                        self.outputs.get(&edge.from_node).is_some_and(|paths| intersects(paths, path))
                            || trigger_passthrough(self.graph, &edge.from_node, path, self.graph.nodes.len())
                    }),
                _ => false,
            })
    }
}

fn output_path(path: &[String]) -> &[String] {
    match path.first().map(String::as_str) {
        Some("item") => &path[1..],
        Some("items") if path.get(1).is_some_and(|key| key.parse::<usize>().is_ok()) => &path[2..],
        _ => path,
    }
}

fn intersects(unknown: &Paths, path: &[String]) -> bool {
    unknown
        .iter()
        .any(|unknown| path.starts_with(unknown) || unknown.starts_with(path))
}

/// Unset transform fields retain caller provenance; explicit overlays cut it off.
/// Limit traversal to the node count so malformed cycles cannot recurse forever.
fn trigger_passthrough(
    graph: &WorkflowGraph,
    node_id: &str,
    read: &[String],
    remaining: usize,
) -> bool {
    if remaining == 0 {
        return false;
    }
    let Some(node) = graph.nodes.iter().find(|node| node.id == node_id) else {
        return false;
    };
    if node.kind == NodeKind::Trigger {
        return true;
    }
    if node.kind != NodeKind::Transform {
        return false;
    }
    if read.first().is_some_and(|field| {
        node.config
            .get("set")
            .and_then(serde_json::Value::as_object)
            .is_some_and(|set| set.contains_key(field))
    }) {
        return false;
    }
    graph.edges.iter().any(|edge| {
        edge.to_node == node_id && trigger_passthrough(graph, &edge.from_node, read, remaining - 1)
    })
}
