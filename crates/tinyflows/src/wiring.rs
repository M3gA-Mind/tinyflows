//! Author-time checks over a tool's published output schema.

use serde_json::Value;

mod authoring;
pub use authoring::{ToolContract, ToolContractLookup, wiring_warnings};

/// Given a tool's output JSON schema and a `split_out` node's configured path
/// (`"json.data"` or `"json.data.issues"`), resolves whether the path lands on
/// something that is definitely not an array.
///
/// - `Some(true)`: non-array (object or scalar). A `split_out` over it fans out
///   over exactly one item: the classic wrong-array-path signal.
/// - `Some(false)`: array; the path is fine.
/// - `None`: the path cannot be resolved against the schema (an unpublished
///   nested field, or a path missing the `data.` segment). Stay silent rather
///   than guess.
#[must_use]
pub fn schema_says_path_is_non_array(output_schema: &Value, configured_path: &str) -> Option<bool> {
    let relative = configured_path
        .strip_prefix("json.")
        .unwrap_or(configured_path);
    if relative == "data" {
        // Whole-payload access: non-array unless the payload's own root type is
        // literally "array" (a bare-array response).
        let ty = output_schema.get("type").and_then(Value::as_str)?;
        return Some(ty != "array");
    }
    let rest = relative.strip_prefix("data.").filter(|r| !r.is_empty())?;
    let mut node = output_schema;
    for seg in rest.split('.') {
        node = node.get("properties")?.get(seg)?;
    }
    let ty = node.get("type").and_then(Value::as_str)?;
    Some(ty != "array")
}

#[cfg(test)]
#[path = "wiring_tests.rs"]
mod tests;
