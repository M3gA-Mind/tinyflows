use super::*;
use serde_json::json;

fn schema() -> Value {
    json!({"type": "object", "properties": {
        "issues": {"type": "array"},
        "meta": {"type": "object", "properties": {"count": {"type": "integer"}}}
    }})
}

#[test]
fn whole_payload_is_non_array_unless_root_is_array() {
    assert_eq!(
        schema_says_path_is_non_array(&schema(), "json.data"),
        Some(true)
    );
    assert_eq!(
        schema_says_path_is_non_array(&json!({"type": "array"}), "json.data"),
        Some(false)
    );
    assert_eq!(schema_says_path_is_non_array(&json!({}), "json.data"), None);
}

#[test]
fn nested_paths_resolve_through_properties() {
    assert_eq!(
        schema_says_path_is_non_array(&schema(), "json.data.issues"),
        Some(false)
    );
    assert_eq!(
        schema_says_path_is_non_array(&schema(), "json.data.meta"),
        Some(true)
    );
    assert_eq!(
        schema_says_path_is_non_array(&schema(), "json.data.meta.count"),
        Some(true)
    );
}

#[test]
fn unresolvable_paths_stay_silent() {
    assert_eq!(
        schema_says_path_is_non_array(&schema(), "json.issues"),
        None
    );
    assert_eq!(
        schema_says_path_is_non_array(&schema(), "json.data.nope"),
        None
    );
    assert_eq!(schema_says_path_is_non_array(&schema(), "json.data."), None);
}
