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
