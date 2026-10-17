//! Exercises portable helper calls through the actual loaded module ABI.

use serde_json::{Value, json};
use tinyflows_bus::{BUS_NAME, OBJECT_PATH, portability};

pub async fn verify_surface(connection: &tinybus::Connection) {
    let proxy = connection
        .proxy(BUS_NAME, OBJECT_PATH, portability::INTERFACE)
        .unwrap();
    let prepared: Value = proxy
        .call(
            "PrepareHttpRequest",
            (portability::PrepareHttpRequest {
                request: json!({"headers":{"authorization":"inline"}}),
                credential: portability::HttpCredentialTemplate::Basic {
                    username: "alice".into(),
                    secret: "secret".into(),
                },
            },),
        )
        .await
        .unwrap();
    assert_eq!(
        prepared["headers"]["Authorization"],
        "Basic YWxpY2U6c2VjcmV0"
    );
    assert!(prepared["headers"].get("authorization").is_none());

    for request in [json!(null), json!({"headers":"not-an-object"})] {
        let error = proxy
            .call::<Value>(
                "PrepareHttpRequest",
                (portability::PrepareHttpRequest {
                    request,
                    credential: portability::HttpCredentialTemplate::Bearer {
                        secret: "module-secret".into(),
                    },
                },),
            )
            .await
            .expect_err("malformed HTTP input must be rejected by the module");
        assert!(!error.to_string().contains("module-secret"));
    }

    let graph = json!({"name":"event","nodes":[{"id":"t","name":"Trigger","kind":"trigger","config":{
        "trigger_kind":"app_event","toolkit":"GitHub","trigger_slug":"ISSUE_CREATED",
        "inputs":{"literal":"=value"}
    }}],"edges":[]});
    let schedule_graph = json!({"name":"schedule","nodes":[{"id":"t","name":"Trigger","kind":"trigger","config":{
        "trigger_kind":"schedule","schedule":{"kind":"every","every_ms":60000}
    }}],"edges":[]});
    let trigger_details: Value = proxy
        .call("DescribeTrigger", (json!({"graph":schedule_graph}),))
        .await
        .expect("compiled module should describe a schedule trigger");
    assert_eq!(trigger_details["kind"], "schedule");
    assert_eq!(trigger_details["schedule"], json!({"kind":"every","every_ms":60000}));

    for graph in [
        json!({"nodes":[],"edges":[]}),
        json!({"nodes":[
            {"id":"t1","name":"T1","kind":"trigger","config":{"trigger_kind":"schedule","schedule":{"kind":"at","at":"2027-01-01T00:00:00Z"}}},
            {"id":"t2","name":"T2","kind":"trigger","config":{"trigger_kind":"manual"}}
        ],"edges":[]}),
    ] {
        let details: Value = proxy
            .call("DescribeTrigger", (json!({"graph":graph}),))
            .await
            .expect("missing or ambiguous trigger metadata should be a safe absence");
        assert!(details["kind"].is_null());
        assert!(details["schedule"].is_null());
    }

    let malformed_schedule = json!({"nodes":[{"id":"t","name":"Trigger","kind":"trigger","config":{
        "trigger_kind":"schedule","schedule":{"kind":"every","every_ms":"soon"}
    }}],"edges":[]});
    assert!(proxy
        .call::<Value>("DescribeTrigger", (json!({"graph":malformed_schedule}),))
        .await
        .is_err(), "malformed schedule metadata must fail closed");

    let matches: bool = proxy
        .call(
            "AppEventBinding",
            (portability::AppEventBinding {
                graph: graph.clone(),
                toolkit: "github".into(),
                trigger_slug: "issue_created".into(),
            },),
        )
        .await
        .unwrap();
    assert!(matches);
    let inputs: Value = proxy
        .call("PinnedInputs", (portability::PinnedInputs { graph },))
        .await
        .unwrap();
    assert_eq!(inputs, json!({"literal":"=value"}));
}
