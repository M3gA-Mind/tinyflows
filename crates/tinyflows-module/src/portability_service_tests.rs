use super::*;
use serde_json::json;

#[tokio::test]
async fn inspect_http_request_matches_engine_parsing_for_outer_whitespace() {
    let details = Portability
        .inspect_http_request(tinyflows_bus::portability::InspectHttpRequest {
            request: json!({"method": "GET", "url": "https://example.com/"}),
            connection_ref: Some(" http_cred:payments".to_owned()),
        })
        .await
        .expect("padded recognized credential reference should inspect");

    assert_eq!(details.credential_name.as_deref(), Some("payments"));
    assert_eq!(details.redacted_summary, "GET https://example.com/");
}

#[tokio::test]
async fn inspect_http_request_keeps_absent_foreign_and_empty_reference_semantics() {
    let inspect = |connection_ref| {
        Portability.inspect_http_request(tinyflows_bus::portability::InspectHttpRequest {
            request: json!({"method": "GET", "url": "https://example.com/"}),
            connection_ref,
        })
    };

    assert_eq!(inspect(None).await.unwrap().credential_name, None);
    assert_eq!(
        inspect(Some("composio:abc".to_owned()))
            .await
            .unwrap()
            .credential_name,
        None
    );
    assert!(inspect(Some(" http_cred: ".to_owned())).await.is_err());
}

#[tokio::test]
async fn completion_helpers_keep_prompt_and_response_formatting_in_the_module() {
    let request = json!({
        "prompt": "summarize",
        "input_context": {"subject": "report"},
        "model": "hint:reasoning",
        "response_format": "json"
    });
    let prepared = Portability
        .prepare_llm_complete(tinyflows_bus::portability::PrepareLlmComplete {
            request: request.clone(),
        })
        .await
        .unwrap();
    assert_eq!(prepared.node_model.as_deref(), Some("hint:reasoning"));
    assert!(prepared.structured_output);
    assert_eq!(prepared.messages.len(), 3);
    assert_eq!(prepared.messages[0].role, "user");
    assert!(prepared.messages[0].content.contains("report"));
    assert_eq!(prepared.messages[1].role, "system");
    assert_eq!(prepared.messages[2].content, "summarize");

    let formatted = Portability
        .format_llm_complete(tinyflows_bus::portability::FormatLlmComplete {
            request: request.clone(),
            response: json!({"text": r#"The result is {"ok":true}"#, "usage": {"input_tokens": 4}}),
        })
        .await
        .unwrap();
    assert_eq!(formatted, json!({"ok": true}));

    let fallback = json!({"text": "no json", "usage": {"input_tokens": 4}});
    let formatted = Portability
        .format_llm_complete(tinyflows_bus::portability::FormatLlmComplete {
            request,
            response: fallback.clone(),
        })
        .await
        .unwrap();
    assert_eq!(formatted, fallback);
}

#[tokio::test]
async fn agent_helpers_keep_model_prompt_timeout_and_result_formatting_in_the_module() {
    let request = json!({
        "prompt": "classify",
        "input_context": {"subject": "invoice"},
        "model": "",
        "timeout_secs": 45,
        "output_parser": {"schema": {"type": "object"}}
    });
    let prepared = Portability
        .prepare_agent_run(tinyflows_bus::portability::PrepareAgentRun {
            request: request.clone(),
            entry_model: Some("hint:reasoning".into()),
            system_prompt: Some("Custom persona".into()),
            effective_iteration_cap: 50,
        })
        .await
        .unwrap();
    assert_eq!(prepared.model.as_deref(), Some("hint:reasoning"));
    assert!(prepared.structured_output);
    assert_eq!(prepared.base_timeout_secs, 45);
    assert_eq!(
        prepared.timeout_secs, 45,
        "explicit timeout is never scaled"
    );
    let context = prepared
        .prompt
        .find("Here is the data from the previous step")
        .expect("module renders upstream data first");
    let steering = prepared
        .prompt
        .find("Respond with a single JSON object only")
        .expect("module adds structured-output steering");
    let persona = prepared
        .prompt
        .find("system: Custom persona")
        .expect("module prepends the host persona to the node messages");
    let task = prepared
        .prompt
        .find("user: classify")
        .expect("module preserves the node task");
    assert!(context < steering && steering < persona && persona < task);
    assert!(prepared.prompt.contains("invoice"));

    let default_timeout = Portability
        .prepare_agent_run(tinyflows_bus::portability::PrepareAgentRun {
            request: json!({"prompt":"classify"}),
            entry_model: None,
            system_prompt: None,
            effective_iteration_cap: 50,
        })
        .await
        .unwrap();
    assert_eq!(default_timeout.base_timeout_secs, 240);
    assert_eq!(default_timeout.timeout_secs, 600);

    let formatted = Portability
        .format_agent_run(tinyflows_bus::portability::FormatAgentRun {
            agent_ref: "classifier".into(),
            request,
            final_text: r#"{"label":"paid"}"#.into(),
        })
        .await
        .unwrap();
    assert_eq!(formatted, json!({"label":"paid"}));
}
