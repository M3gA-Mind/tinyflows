//! Trail-off fallback construction over host-correlated tool results.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// One chronological tool result, after the host has correlated call ids to
/// names. No transcript or harness dependency crosses this boundary.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ToolResult {
    /// The issuing tool's name, or `None` when no call could be correlated.
    pub tool_name: Option<String>,
    /// Result body exactly as the authoring tool returned it.
    pub content: String,
}

/// Authoring results that can explain why a builder stopped without a proposal.
pub const BLOCKER_TOOLS: &[&str] = &[
    "dry_run_workflow",
    "validate_workflow",
    "propose_workflow",
    "revise_workflow",
    "edit_workflow",
    "save_workflow",
    "create_workflow",
    "duplicate_flow",
];

/// Describe the newest authoring result if it failed. A newer authoring
/// success is authoritative and never resurrects an earlier failure.
#[must_use]
pub fn last_builder_tool_blocker(history: &[ToolResult]) -> Option<String> {
    let result = history.iter().rev().find(|result| {
        result
            .tool_name
            .as_deref()
            .is_some_and(|name| BLOCKER_TOOLS.contains(&name))
    })?;
    let description = describe_tool_result_blocker(&result.content)?;
    Some(match description.char_indices().nth(500) {
        Some((index, _)) => format!("{}...", description[..index].trim_end()),
        None => description,
    })
}

fn describe_tool_result_blocker(content: &str) -> Option<String> {
    let trimmed = content.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Ok(value) = serde_json::from_str::<Value>(trimmed) {
        if value.get("type").and_then(Value::as_str) == Some("workflow_proposal") {
            return None;
        }
        return value
            .get("ok")
            .and_then(Value::as_bool)
            .filter(|ok| !ok)
            .map(|_| value.to_string());
    }
    Some(trimmed.to_owned())
}

/// Ask an answerable fallback question using the last authoring blocker, or
/// the existing generic question when there is no confidently identified one.
#[must_use]
pub fn build_trail_off_fallback(history: &[ToolResult]) -> String {
    match last_builder_tool_blocker(history) {
        Some(blocker) => format!(
            "I wasn't able to finish building this workflow. Here's where I got stuck:\n\n{blocker}\n\n\
             Could you tell me how you'd like me to resolve that, or share more detail about what's needed here?"
        ),
        None => "I wasn't able to finish building this workflow in this turn. Could you describe \
                  what you'd like in more detail, or tell me which part to focus on?"
            .to_owned(),
    }
}

/// Prepend the fallback while retaining the model's own words. A silent turn
/// returns only the fallback, without an empty divider.
#[must_use]
pub fn combine_trail_off_fallback(fallback: &str, original: &str) -> String {
    let original = original.trim();
    if original.is_empty() {
        fallback.to_owned()
    } else {
        format!("{fallback}\n\n---\n\n{original}")
    }
}

#[cfg(test)]
#[path = "fallback_tests.rs"]
mod tests;
