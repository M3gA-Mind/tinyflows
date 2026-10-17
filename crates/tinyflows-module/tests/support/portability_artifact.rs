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

    let graph = json!({"name":"event","nodes":[{"id":"t","name":"Trigger","kind":"trigger","config":{
        "trigger_kind":"app_event","toolkit":"GitHub","trigger_slug":"ISSUE_CREATED",
        "inputs":{"literal":"=value"}
    }}],"edges":[]});
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
