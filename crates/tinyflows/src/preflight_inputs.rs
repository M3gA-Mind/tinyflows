//! Mock child input values and path-level provenance of unavailable runtime data.

use serde_json::{Map, Value, json};

use crate::model::{InputType, WorkflowGraph};

#[derive(Clone, Default)]
pub(super) struct MockInputs {
    pub(super) values: Map<String, Value>,
    unknown: Vec<Vec<String>>,
}

impl MockInputs {
    /// Root declared inputs are supplied by a real caller, outside this mock run.
    pub(super) fn root(graph: &WorkflowGraph) -> Self {
        Self {
            values: graph
                .inputs
                .iter()
                .filter(|input| input.required)
                .map(|input| (input.name.clone(), placeholder(input.ty)))
                .collect(),
            unknown: graph
                .inputs
                .iter()
                .map(|input| vec![input.name.clone()])
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
            let mut value = crate::expr::resolve(raw, &json!({"inputs": self.values}));
            self.collect_unknown(raw, &[input.name.clone()], &mut child.unknown);
            if raw.as_str().is_some_and(crate::expr::is_expression) && !input.ty.accepts(&value) {
                value = placeholder(input.ty);
            }
            child.values.insert(input.name.clone(), value);
        }
        child
    }

    /// A read intersecting an opaque input path cannot prove a field is absent.
    pub(super) fn is_unverifiable(&self, expression: &str) -> bool {
        input_path(expression).is_some_and(|path| {
            self.unknown
                .iter()
                .any(|unknown| path.starts_with(unknown) || unknown.starts_with(&path))
        })
    }

    /// Transfer only the unavailable leaves when JSON mixes literal and dynamic fields.
    fn collect_unknown(&self, raw: &Value, destination: &[String], out: &mut Vec<Vec<String>>) {
        match raw {
            Value::Object(values) => {
                for (key, value) in values {
                    let mut path = destination.to_vec();
                    path.push(key.clone());
                    self.collect_unknown(value, &path, out);
                }
            }
            Value::Array(values) => {
                for (index, value) in values.iter().enumerate() {
                    let mut path = destination.to_vec();
                    path.push(index.to_string());
                    self.collect_unknown(value, &path, out);
                }
            }
            Value::String(expression) if crate::expr::is_expression(expression) => {
                if let Some(source) = input_path(expression) {
                    if source.is_empty() && !self.unknown.is_empty() {
                        out.push(destination.to_vec());
                        return;
                    }
                    for unknown in &self.unknown {
                        if source.starts_with(unknown) {
                            out.push(destination.to_vec());
                        } else if let Some(suffix) = unknown.strip_prefix(source.as_slice()) {
                            let mut path = destination.to_vec();
                            path.extend_from_slice(suffix);
                            out.push(path);
                        }
                    }
                } else {
                    // Runtime scopes are absent from this inputs-only mock.
                    out.push(destination.to_vec());
                }
            }
            _ => {}
        }
    }
}

/// Recognize input paths in shorthand, bracket notation, and jq field-access pipes.
/// Other jq operators conservatively address the entire input scope.
fn input_path(expression: &str) -> Option<Vec<String>> {
    let body = expression.strip_prefix('=')?.trim();
    let body = body.strip_prefix('.').unwrap_or(body);
    let mut rest = body.strip_prefix("inputs")?;
    if !rest.is_empty() && !rest.starts_with(['.', '[', ' ', '|']) {
        return None;
    }
    let mut path = Vec::new();
    while !rest.trim().is_empty() {
        rest = rest.trim_start();
        if let Some(tail) = rest.strip_prefix('|') {
            rest = tail.trim_start();
            if !rest.starts_with(['.', '[']) {
                return Some(Vec::new());
            }
        }
        if let Some(tail) = rest.strip_prefix('.') {
            let end = tail
                .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == '-'))
                .unwrap_or(tail.len());
            if end == 0 {
                return Some(Vec::new());
            }
            path.push(tail[..end].to_string());
            rest = &tail[end..];
        } else if let Some(tail) = rest.strip_prefix('[') {
            let Some(end) = tail.find(']') else {
                return Some(Vec::new());
            };
            let key = tail[..end].trim();
            let key = if key.starts_with('"') {
                let Ok(key) = serde_json::from_str::<String>(key) else {
                    return Some(Vec::new());
                };
                key
            } else if let Some(key) = key
                .strip_prefix('\'')
                .and_then(|key| key.strip_suffix('\''))
            {
                key.to_string()
            } else if key.chars().all(|c| c.is_ascii_digit()) && !key.is_empty() {
                key.to_string()
            } else {
                return Some(Vec::new());
            };
            path.push(key);
            rest = &tail[end + 1..];
        } else {
            return Some(Vec::new());
        }
    }
    Some(path)
}

/// A type-correct value lets the mock run reach unrelated tool bindings.
fn placeholder(ty: InputType) -> Value {
    match ty {
        InputType::String => json!("preflight"),
        InputType::Number => json!(0),
        InputType::Boolean => json!(false),
        InputType::Json => json!({}),
    }
}
