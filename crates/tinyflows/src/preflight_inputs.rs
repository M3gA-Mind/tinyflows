//! Mock child input values and the provenance of unavailable runtime data.

use std::collections::HashSet;

use serde_json::{Map, Value, json};

use crate::model::{InputType, WorkflowGraph};

#[derive(Clone, Default)]
pub(super) struct MockInputs {
    pub(super) values: Map<String, Value>,
    unknown: HashSet<String>,
}

impl MockInputs {
    /// Root declared inputs are supplied by a real caller, outside this mock run.
    pub(super) fn root(graph: &WorkflowGraph) -> Self {
        Self {
            values: Map::new(),
            unknown: graph
                .inputs
                .iter()
                .map(|input| input.name.clone())
                .collect(),
        }
    }

    /// Preserve literals and known parent inputs; retain uncertainty across child mappings.
    pub(super) fn child(&self, graph: &WorkflowGraph, config: &Value) -> Self {
        let mut child = Self::default();
        let Some(supplied) = config.get("inputs").and_then(Value::as_object) else {
            return child;
        };
        for input in &graph.inputs {
            let Some(raw) = supplied.get(&input.name) else {
                continue;
            };
            let (mut value, misses) =
                crate::expr::resolve_traced(raw, &json!({"inputs": self.values}));
            // JSON accepts null, so type acceptance alone cannot establish that
            // an expression resolved from the inputs-only synthetic scope.
            if !misses.is_empty() || self.contains_unknown_read(raw) {
                child.unknown.insert(input.name.clone());
                if !input.ty.accepts(&value) {
                    value = match input.ty {
                        InputType::String => json!("preflight"),
                        InputType::Number => json!(0),
                        InputType::Boolean => json!(false),
                        InputType::Json => json!({}),
                    };
                }
            }
            child.values.insert(input.name.clone(), value);
        }
        child
    }

    /// A nested read from an opaque input cannot prove that a field is absent.
    pub(super) fn is_unverifiable(&self, expression: &str) -> bool {
        let Some(body) = expression.strip_prefix('=') else {
            return false;
        };
        let body = body.trim().trim_start_matches('.');
        let Some(rest) = body.strip_prefix("inputs") else {
            return false;
        };
        let name = if let Some(rest) = rest.strip_prefix('.') {
            rest.split(['.', '[', ' ', '|']).next().unwrap_or_default()
        } else if let Some(rest) = rest.strip_prefix('[') {
            rest.trim_start()
                .trim_start_matches(['"', '\''])
                .split(['"', '\'', ']'])
                .next()
                .unwrap_or_default()
        } else {
            return rest.is_empty() && !self.unknown.is_empty();
        };
        self.unknown.contains(name)
    }

    fn contains_unknown_read(&self, value: &Value) -> bool {
        match value {
            Value::String(expression) => self.is_unverifiable(expression),
            Value::Object(values) => values
                .values()
                .any(|value| self.contains_unknown_read(value)),
            Value::Array(values) => values.iter().any(|value| self.contains_unknown_read(value)),
            _ => false,
        }
    }
}
