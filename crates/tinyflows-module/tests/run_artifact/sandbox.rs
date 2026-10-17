use super::*;

use std::sync::atomic::{AtomicUsize, Ordering};
use tinybus::{broker::Broker, module::ModuleHost, transport::memory::MemoryTransport};
use tinyflows_bus::runs::{self, CallbackAddress, SandboxRun};

#[derive(Clone)]
struct Preflight(Arc<AtomicUsize>);

#[tinybus::interface(name = "ai.tinyhumans.tinyflows.CapsHost")]
impl Preflight {
    async fn tool_invoke(
        &self,
        slug: String,
        args: serde_json::Value,
        _connection: Option<String>,
    ) -> tinybus::Result<serde_json::Value> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(json!({"sandbox_echo": true, "slug": slug, "args": args}))
    }
}

fn agent_graph(named: bool) -> serde_json::Value {
    let mut config = json!({
        "prompt": "Write a subject",
        "output_parser": {"schema": {"type":"object","properties":{"subject":{"type":"string"}}}}
    });
    if named {
        config["agent_ref"] = json!("named-agent");
    }
    json!({
        "nodes":[
            {"id":"trigger","name":"Trigger","kind":"trigger","config":{}},
            {"id":"agent","name":"Agent","kind":"agent","config":config}
        ],
        "edges":[{"from_node":"trigger","to_node":"agent"}]
    })
}

#[tokio::test]
#[ignore = "run through scripts/verify-module-artifact.sh"]
async fn compiled_module_sandbox_uses_schema_mocks_and_only_the_preflight_callback() {
    assert_eq!(runs::CONTRACT_VERSION, 3);
    assert!(runs::METHODS.contains(&runs::methods::SANDBOX_RUN));
    let artifact = std::env::var_os("TINYFLOWS_MODULE_ARTIFACT")
        .expect("set TINYFLOWS_MODULE_ARTIFACT to the built cdylib");
    let broker = Broker::new();
    let module_host = ModuleHost::new(broker.clone());
    module_host
        .load_file(std::path::PathBuf::from(artifact))
        .unwrap();
    let (client, transport) = MemoryTransport::pair();
    broker.attach(Arc::new(transport));
    let connection = Connection::connect(Box::new(client)).await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    connection
        .serve_at(
            "/test/SandboxPreflight".try_into().unwrap(),
            Preflight(calls.clone()),
        )
        .await
        .unwrap();
    connection
        .request_name("test.SandboxPreflight")
        .await
        .unwrap();
    let proxy = connection
        .proxy(tinyflows_bus::BUS_NAME, runs::OBJECT_PATH, runs::INTERFACE)
        .unwrap();
    let callback = CallbackAddress {
        destination: "test.SandboxPreflight".into(),
        object_path: "/test/SandboxPreflight".into(),
    };

    for named in [false, true] {
        let response: runs::SandboxRunResponse = proxy
            .call(
                runs::methods::SANDBOX_RUN,
                (SandboxRun {
                    graph: agent_graph(named),
                    input: json!({}),
                    tool_invoker: callback.clone(),
                },),
            )
            .await
            .unwrap();
        assert_eq!(response.result["sandbox"], true);
        assert_eq!(response.result["ok"], true, "{response:?}");
        assert_eq!(
            response.result["output"]["nodes"]["agent"]["item"]["json"]["subject"], "",
            "{response:?}"
        );
    }

    let tool_graph = json!({
        "nodes":[
            {"id":"trigger","name":"Trigger","kind":"trigger","config":{}},
            {"id":"echo","name":"Echo","kind":"tool_call","config":{"slug":"demo:echo","args":{"text":"hi"}}}
        ],
        "edges":[{"from_node":"trigger","to_node":"echo"}]
    });
    let response: runs::SandboxRunResponse = proxy
        .call(
            runs::methods::SANDBOX_RUN,
            (SandboxRun {
                graph: tool_graph,
                input: json!({}),
                tool_invoker: callback,
            },),
        )
        .await
        .unwrap();
    assert_eq!(response.result["ok"], true, "{response:?}");
    assert_eq!(calls.load(Ordering::SeqCst), 1, "agent paths are mock-only");
    assert!(
        response.result["output"]
            .to_string()
            .contains("sandbox_echo")
    );
}
