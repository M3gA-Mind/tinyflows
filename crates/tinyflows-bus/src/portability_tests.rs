use super::*;
use serde_json::json;

#[test]
fn portability_wire_requests_round_trip_without_credential_diagnostics() {
    let request = PrepareHttpRequest {
        request: json!({"headers":null}),
        credential: HttpCredentialTemplate::Bearer {
            secret: "secret".into(),
        },
    };
    let wire = serde_json::to_value(&request).unwrap();
    assert_eq!(wire["credential"]["scheme"], "bearer");
    assert!(serde_json::from_value::<PrepareHttpRequest>(wire).unwrap() == request);
    let binding = AppEventBinding {
        graph: json!({"nodes":[]}),
        toolkit: "github".into(),
        trigger_slug: "ISSUE_CREATED".into(),
    };
    assert_eq!(serde_json::from_value::<AppEventBinding>(serde_json::to_value(&binding).unwrap()).unwrap(), binding);
    let pinned = PinnedInputs { graph: json!({"nodes":[]}) };
    assert_eq!(serde_json::from_value::<PinnedInputs>(serde_json::to_value(&pinned).unwrap()).unwrap(), pinned);
}
