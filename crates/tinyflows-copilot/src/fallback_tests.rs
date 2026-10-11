use super::*;

fn result(tool_name: &str, content: &str) -> ToolResult {
    ToolResult {
        tool_name: Some(tool_name.into()),
        content: content.into(),
    }
}

#[test]
fn new_flow_and_clone_failures_reach_the_trail_off_fallback() {
    for tool in ["create_workflow", "duplicate_flow"] {
        let history = [result(tool, "An account is required to save this flow")];
        assert_eq!(
            last_builder_tool_blocker(&history).as_deref(),
            Some("An account is required to save this flow"),
            "{tool}"
        );
        assert!(build_trail_off_fallback(&history).contains("An account is required"));
    }
}

#[test]
fn a_new_flow_or_clone_success_supersedes_an_earlier_failure() {
    for tool in ["create_workflow", "duplicate_flow"] {
        let history = [
            result("validate_workflow", "broken"),
            result(tool, r#"{"ok":true}"#),
        ];
        assert_eq!(last_builder_tool_blocker(&history), None, "{tool}");
    }
}

#[test]
fn latest_authoring_success_does_not_resurrect_an_earlier_blocker() {
    let history = [
        result("validate_workflow", "broken"),
        result("validate_workflow", r#"{"ok":true}"#),
    ];
    assert_eq!(last_builder_tool_blocker(&history), None);
}

#[test]
fn unrelated_read_result_does_not_explain_a_builder_failure() {
    let history = [
        result("validate_workflow", "broken"),
        result("get_flow", "a saved flow"),
    ];
    assert_eq!(
        last_builder_tool_blocker(&history).as_deref(),
        Some("broken")
    );
}

#[test]
fn false_report_keeps_entire_payload_as_blocker() {
    let history = [result(
        "dry_run_workflow",
        r#"{"ok":false,"problems":["null input"]}"#,
    )];
    let blocker = last_builder_tool_blocker(&history).unwrap();
    let value: serde_json::Value = serde_json::from_str(&blocker).unwrap();
    assert_eq!(value["problems"][0], "null input");
}

#[test]
fn proposal_is_success_even_if_payload_also_has_false_ok_marker() {
    let history = [result(
        "propose_workflow",
        r#"{"type":"workflow_proposal","ok":false}"#,
    )];
    assert_eq!(last_builder_tool_blocker(&history), None);
}

#[test]
fn unrecognized_or_uncorrelated_results_are_not_blockers() {
    for output in [
        result("validate_workflow", r#"{"progress":"pending"}"#),
        ToolResult {
            tool_name: None,
            content: "failure".into(),
        },
        result("save_workflow", " "),
    ] {
        assert_eq!(last_builder_tool_blocker(&[output]), None);
    }
}

#[test]
fn fallback_preserves_original_model_text_and_exact_user_copy() {
    let fallback = build_trail_off_fallback(&[result("edit_workflow", "Need an account")]);
    assert_eq!(
        fallback,
        "I wasn't able to finish building this workflow. Here's where I got stuck:\n\nNeed an account\n\nCould you tell me how you'd like me to resolve that, or share more detail about what's needed here?"
    );
    assert_eq!(
        combine_trail_off_fallback(&fallback, " Original response \n"),
        format!("{fallback}\n\n---\n\nOriginal response")
    );
    assert_eq!(combine_trail_off_fallback(&fallback, " \n "), fallback);
}

#[test]
fn generic_fallback_keeps_existing_copy() {
    assert_eq!(
        build_trail_off_fallback(&[]),
        "I wasn't able to finish building this workflow in this turn. Could you describe what you'd like in more detail, or tell me which part to focus on?"
    );
}

#[test]
fn blocker_is_bounded_by_unicode_characters() {
    let history = [result("validate_workflow", &"💡".repeat(501))];
    let blocker = last_builder_tool_blocker(&history).unwrap();
    assert_eq!(blocker, format!("{}...", "💡".repeat(500)));
}
