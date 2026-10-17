//! Best-effort wiring diagnostics driven by host-published tool contracts.

use crate::{
    bindings::{collect_expressions, parse_node_binding},
    model::{NodeKind, WorkflowGraph},
};
use serde_json::Value;
use std::collections::HashMap;

/// Contract data the host knows about one tool's output.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ToolContract {
    /// Known payload fields.
    pub output_fields: Vec<String>,
    /// Output schema, when published.
    pub output_schema: Option<Value>,
    /// Known primary array path, relative to the payload unless already prefixed.
    pub primary_array_path: Option<String>,
    /// Payload envelope segment.
    pub envelope_prefix: Option<String>,
    /// Envelope metadata fields outside the payload.
    pub metadata_fields: Vec<String>,
}

/// Scope-bound lookup supplied by a host; missing or unavailable contracts stay silent.
#[async_trait::async_trait]
pub trait ToolContractLookup: Send + Sync {
    /// Resolve the published contract for a tool slug.
    async fn lookup(&self, slug: &str) -> Option<ToolContract>;
}

/// Return advisory output-field, envelope, and `split_out` path warnings.
pub async fn wiring_warnings(
    graph: &WorkflowGraph,
    lookup: &dyn ToolContractLookup,
) -> Vec<String> {
    let mut contracts = HashMap::new();
    for node in &graph.nodes {
        if node.kind == NodeKind::ToolCall
            && let Some(slug) = node.config.get("slug").and_then(Value::as_str)
            && !slug.starts_with('=')
        {
            contracts.entry(slug.to_string()).or_insert_with(|| None);
        }
    }
    for (slug, contract) in &mut contracts {
        *contract = lookup.lookup(slug).await;
    }

    let mut warnings = Vec::new();
    for node in &graph.nodes {
        for (location, expr) in collect_expressions(&node.config) {
            let Some(binding) = parse_node_binding(&expr) else {
                continue;
            };
            if !binding.through_envelope {
                continue;
            }
            let Some(source) = graph.node(&binding.node_id) else {
                continue;
            };
            if source.kind != NodeKind::ToolCall {
                continue;
            }
            let Some(slug) = source.config.get("slug").and_then(Value::as_str) else {
                continue;
            };
            let Some(contract) = contracts.get(slug).and_then(Option::as_ref) else {
                continue;
            };
            if contract.output_schema.is_none() && contract.output_fields.is_empty() {
                continue;
            }
            if contract.metadata_fields.iter().any(|field| {
                binding.field_path == *field || binding.field_path.starts_with(&format!("{field}."))
            }) {
                continue;
            }
            let Some(prefix) = contract
                .envelope_prefix
                .as_deref()
                .filter(|p| !p.is_empty())
            else {
                check_output_field(
                    &mut warnings,
                    node,
                    &location,
                    &expr,
                    &binding.field_path,
                    &binding.node_id,
                    slug,
                    contract,
                );
                continue;
            };
            if binding.field_path == prefix {
                continue;
            }
            let Some(payload_path) = binding.field_path.strip_prefix(&format!("{prefix}.")) else {
                warnings.push(format!(
                    "Node '{}': binding `{location}` (`{expr}`) reads `.item.json.{}` off tool_call `{}` (`{slug}`), but its payload is wrapped in `{prefix}`. Bind via `=nodes.{}.item.json.{prefix}.{}` instead.",
                    node.id, binding.field_path, binding.node_id, binding.node_id, binding.field_path
                ));
                continue;
            };
            check_output_field(
                &mut warnings,
                node,
                &location,
                &expr,
                payload_path,
                &binding.node_id,
                slug,
                contract,
            );
        }
    }

    for node in &graph.nodes {
        if node.kind != NodeKind::SplitOut {
            continue;
        }
        let configured = node.config.get("path").and_then(Value::as_str);
        for edge in graph.edges.iter().filter(|edge| edge.to_node == node.id) {
            let Some(source) = graph.node(&edge.from_node) else {
                continue;
            };
            if source.kind != NodeKind::ToolCall {
                continue;
            }
            let Some(slug) = source.config.get("slug").and_then(Value::as_str) else {
                continue;
            };
            let Some(contract) = contracts.get(slug).and_then(Option::as_ref) else {
                continue;
            };
            if contract.output_schema.is_none() && contract.primary_array_path.is_none() {
                continue;
            }
            if let Some(primary) = contract.primary_array_path.as_deref() {
                let expected = expected_array_path(contract, primary);
                if configured != Some(expected.as_str()) {
                    let shown = configured
                        .map(|path| format!("\"{path}\""))
                        .unwrap_or_else(|| "unset".into());
                    warnings.push(format!("Node '{}': split_out.path is {shown} but tool_call `{}` (`{slug}`) publishes its array at `{expected}`; set config.path to `{expected}`.", node.id, source.id));
                }
            } else if let (Some(path), Some(schema)) = (configured, contract.output_schema.as_ref())
                && schema_says_path_is_non_array(schema, path, contract.envelope_prefix.as_deref())
                    == Some(true)
            {
                warnings.push(format!("Node '{}': split_out.path is `{path}` but tool_call `{}` (`{slug}`) has a known non-array value there, so it fans out over one item. Check the tool's published output contract.", node.id, source.id));
            }
        }
    }
    warnings
}

fn expected_array_path(contract: &ToolContract, path: &str) -> String {
    let relative = path.strip_prefix("json.").unwrap_or(path);
    match contract
        .envelope_prefix
        .as_deref()
        .filter(|prefix| !prefix.is_empty())
    {
        Some(prefix) if relative == prefix || relative.starts_with(&format!("{prefix}.")) => {
            format!("json.{relative}")
        }
        Some(prefix) => format!("json.{prefix}.{relative}"),
        None => format!("json.{relative}"),
    }
}

fn check_output_field(
    warnings: &mut Vec<String>,
    node: &crate::model::Node,
    location: &str,
    expr: &str,
    field_path: &str,
    source_id: &str,
    slug: &str,
    contract: &ToolContract,
) {
    let field = field_path.split('.').next().unwrap_or(field_path);
    if !contract.output_fields.iter().any(|known| known == field) {
        warnings.push(format!("Node '{}': binding `{location}` (`{expr}`) reads field `{field}` from tool_call `{source_id}` (`{slug}`), but it is absent from the published output fields ({}). Check the tool's output contract.", node.id, contract.output_fields.join(", ")));
    }
}

fn schema_says_path_is_non_array(
    schema: &Value,
    configured: &str,
    prefix: Option<&str>,
) -> Option<bool> {
    let mut relative = configured.strip_prefix("json.").unwrap_or(configured);
    if let Some(prefix) = prefix {
        if relative == prefix {
            let ty = schema.get("type").and_then(Value::as_str)?;
            return Some(ty != "array");
        }
        relative = relative.strip_prefix(&format!("{prefix}."))?;
    } else if relative == "data" {
        let ty = schema.get("type").and_then(Value::as_str)?;
        return Some(ty != "array");
    }
    let mut current = schema;
    for segment in relative.split('.') {
        current = current.get("properties")?.get(segment)?;
    }
    let ty = current.get("type").and_then(Value::as_str)?;
    Some(ty != "array")
}

#[cfg(test)]
#[path = "authoring_tests.rs"]
mod tests;
