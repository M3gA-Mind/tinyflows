//! The save-time approval manifest: every permission prompt a run of a graph
//! will raise, computed statically so a save+enable card can ask for all of
//! them at once instead of parking the run node by node.
//!
//! Split in two so the host keeps the policy. [`manifest_candidates`] walks the
//! graph and names every node that could be gated; the host classifies each
//! (its tiers, its tool registry, its action catalogue; possibly async) and
//! [`assemble_manifest`] turns the answers into the wire entries.

use std::collections::HashSet;

use serde_json::{Value, json};
use tinyflows::model::{NodeKind, WorkflowGraph};
pub use tinyflows_bus::catalog::{ManifestCandidate, ManifestClass};

/// Walks `graph` and lists every node that could raise an approval prompt, in
/// graph order. `native_prefix` is the slug prefix of host-native tools.
#[must_use]
pub fn manifest_candidates(graph: &WorkflowGraph, native_prefix: &str) -> Vec<ManifestCandidate> {
    let mut out = Vec::new();
    for node in &graph.nodes {
        match node.kind {
            NodeKind::HttpRequest => out.push(ManifestCandidate::Http {
                node_id: node.id.clone(),
                url: node
                    .config
                    .get("url")
                    .and_then(Value::as_str)
                    .unwrap_or("HTTP request")
                    .to_string(),
            }),
            NodeKind::Code => out.push(ManifestCandidate::Code {
                node_id: node.id.clone(),
            }),
            NodeKind::ToolCall => {
                let slug = node.config.get("slug").and_then(Value::as_str);
                match slug {
                    Some(s) if s.trim_start().starts_with('=') => {
                        out.push(ManifestCandidate::DynamicTool {
                            node_id: node.id.clone(),
                        });
                    }
                    Some(s) if s.starts_with(native_prefix) => {
                        let tool_name = s.trim_start_matches(native_prefix).trim().to_string();
                        if tool_name.is_empty() {
                            continue; // structurally invalid; validate rejects it
                        }
                        out.push(ManifestCandidate::NativeTool {
                            node_id: node.id.clone(),
                            tool_name,
                            args: node.config.get("args").cloned().unwrap_or(json!({})),
                        });
                    }
                    Some(s) if !s.trim().is_empty() => out.push(ManifestCandidate::ConnectorTool {
                        node_id: node.id.clone(),
                        slug: s.to_string(),
                    }),
                    _ => {}
                }
            }
            NodeKind::Agent
                if node
                    .config
                    .get("agent_ref")
                    .and_then(Value::as_str)
                    .is_some_and(|r| !r.trim().is_empty()) =>
            {
                out.push(ManifestCandidate::Agent {
                    node_id: node.id.clone(),
                });
            }
            _ => {}
        }
    }
    out
}

/// Builds the manifest entries from `candidates` and the host's `classes`
/// (index-aligned; entries for [`ManifestCandidate::DynamicTool`] and
/// [`ManifestCandidate::Agent`] are ignored, and a missing class skips the
/// candidate).
///
/// Entry kinds: `approvable` (will park; pre-approving `tool_name` clears it),
/// `blocked` (the policy refuses the class; informational), `dynamic` (slug
/// resolved at run time, best-effort disclosure), `agent` (inner calls cannot be
/// enumerated). Approvable and blocked rows dedupe on `tool_name`: two nodes
/// calling the same tool need one grant.
#[must_use]
pub fn assemble_manifest(
    candidates: &[ManifestCandidate],
    classes: &[Option<ManifestClass>],
) -> Vec<Value> {
    let mut entries = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    let mut push_gated = |entries: &mut Vec<Value>,
                          node_id: &str,
                          tool_name: String,
                          label: String,
                          class: &ManifestClass| {
        if class.skip || !seen.insert(tool_name.clone()) {
            return;
        }
        entries.push(json!({
            "kind": if class.blocked { "blocked" } else { "approvable" },
            "node_id": node_id,
            "tool_name": tool_name,
            "label": label,
            "class": class.name,
        }));
    };
    for (i, candidate) in candidates.iter().enumerate() {
        let class = classes.get(i).and_then(Option::as_ref);
        match candidate {
            ManifestCandidate::Http { node_id, url } => {
                if let Some(class) = class {
                    push_gated(
                        &mut entries,
                        node_id,
                        "flows_http_request".into(),
                        format!("Call {url}"),
                        class,
                    );
                }
            }
            ManifestCandidate::Code { node_id } => {
                if let Some(class) = class {
                    push_gated(
                        &mut entries,
                        node_id,
                        "flows_code".into(),
                        "Run sandboxed code".into(),
                        class,
                    );
                }
            }
            ManifestCandidate::NativeTool {
                node_id, tool_name, ..
            } => {
                if let Some(class) = class {
                    push_gated(
                        &mut entries,
                        node_id,
                        tool_name.clone(),
                        format!("Use tool {tool_name}"),
                        class,
                    );
                }
            }
            ManifestCandidate::ConnectorTool { node_id, slug } => {
                if let Some(class) = class {
                    push_gated(
                        &mut entries,
                        node_id,
                        slug.clone(),
                        format!("Use {slug}"),
                        class,
                    );
                }
            }
            ManifestCandidate::DynamicTool { node_id } => entries.push(json!({
                "kind": "dynamic",
                "node_id": node_id,
                "label": "Tool chosen at run time",
            })),
            ManifestCandidate::Agent { node_id } => entries.push(json!({
                "kind": "agent",
                "node_id": node_id,
                "label": "AI step — may ask for permission for its own actions",
            })),
        }
    }
    entries
}

/// Splits a manifest's approvable entries into the trust keys a run will have to
/// ask for (`missing`) and the ones the flow already holds a grant for.
///
/// With no gate installed both lists are empty: nothing ever parks, so nothing is
/// missing, and nothing was granted, so nothing is already trusted. Naming the
/// approvable keys there would tell the card that permissions are authorized on
/// a host that holds no grant for them.
#[must_use]
pub fn split_manifest_trust(
    entries: &[Value],
    gate_installed: bool,
    trusted: &HashSet<String>,
) -> (Vec<String>, Vec<String>) {
    let mut missing = Vec::new();
    let mut already_trusted = Vec::new();
    if !gate_installed {
        return (missing, already_trusted);
    }
    for entry in entries {
        if entry.get("kind").and_then(Value::as_str) != Some("approvable") {
            continue;
        }
        let Some(tool_name) = entry.get("tool_name").and_then(Value::as_str) else {
            continue;
        };
        if trusted.contains(tool_name) {
            already_trusted.push(tool_name.to_string());
        } else {
            missing.push(tool_name.to_string());
        }
    }
    (missing, already_trusted)
}

#[cfg(test)]
#[path = "approval_manifest_tests.rs"]
mod tests;
