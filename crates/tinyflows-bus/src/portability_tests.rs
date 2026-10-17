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

#[test]
fn every_credential_template_has_a_stable_tagged_wire_shape() {
    let cases = [
        (
            HttpCredentialTemplate::Bearer {
                secret: "token".into(),
            },
            json!({"scheme":"bearer","secret":"token"}),
        ),
        (
            HttpCredentialTemplate::Basic {
                username: "alice".into(),
                secret: "secret".into(),
            },
            json!({"scheme":"basic","username":"alice","secret":"secret"}),
        ),
        (
            HttpCredentialTemplate::Header {
                header: "X-Key".into(),
                secret: "secret".into(),
            },
            json!({"scheme":"header","header":"X-Key","secret":"secret"}),
        ),
        (
            HttpCredentialTemplate::Resolved {
                header: "Authorization".into(),
                value: "Bearer token".into(),
            },
            json!({"scheme":"resolved","header":"Authorization","value":"Bearer token"}),
        ),
    ];
    for (template, expected) in cases {
        let wire = serde_json::to_value(&template).unwrap();
        assert_eq!(wire, expected);
        assert!(serde_json::from_value::<HttpCredentialTemplate>(wire).unwrap() == template);
    }
}
