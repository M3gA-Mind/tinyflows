use super::*;
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tinyflows_bus::catalog::{
    self, ApplyGraphOpsRequest, ApprovalManifestRequest, ClassifyManifestCandidatesRequest,
    ManifestCandidate, ManifestClass,
};

struct Classifier {
    classes: Vec<Option<ManifestClass>>,
    seen: Arc<Mutex<Vec<ManifestCandidate>>>,
}

#[tinybus::interface(name = "ai.tinyhumans.tinyflows.ApprovalManifestHost")]
impl Classifier {
    async fn classify_manifest_candidates(
        &self,
        request: ClassifyManifestCandidatesRequest,
    ) -> tinybus::Result<Vec<Option<ManifestClass>>> {
        *self.seen.lock().unwrap() = request.candidates;
        Ok(self.classes.clone())
    }
}

struct Resolver(Option<Value>);

#[tinybus::interface(name = "ai.tinyhumans.tinyflows.AuthoringHost")]
impl Resolver {
    async fn resolve_workflow(&self, _: String) -> tinybus::Result<Option<Value>> {
        Ok(self.0.clone())
    }
}

async fn connections() -> (Connection, Connection) {
    let bus = tinybus::transport::memory::MemoryBus::new();
    tinybus::broker::Broker::new().spawn(bus.clone());
    let server = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    crate::service::setup(server.clone()).await.unwrap();
    let client = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    (server, client)
}

#[tokio::test]
async fn catalog_node_kinds_returns_the_full_portable_contracts() -> tinybus::Result<()> {
    let (_server, client) = connections().await;
    let proxy = client.proxy(tinyflows_bus::BUS_NAME, catalog::OBJECT_PATH, catalog::INTERFACE)?;
    let contracts: Vec<tinyflows_bus::node_kinds::NodeKindContract> =
        proxy.call(catalog::members::NODE_KINDS, ()).await?;
    assert_eq!(contracts.len(), tinyflows_bus::node_kinds::NODE_KINDS.len());
    let http = contracts
        .iter()
        .find(|contract| contract.kind == "http_request")
        .unwrap();
    assert!(!http.config_fields.is_empty());
    assert_eq!(http.ports.outputs, ["main"]);
    assert_eq!(http.example["kind"], "http_request");
    assert!(http.notes.is_empty());
    assert_eq!(
        serde_json::to_value(&http).unwrap(),
        json!({
            "kind":"http_request",
            "summary":"Make an HTTP request and expose the parsed response.",
            "description":"Send an HTTP request. The host supplies the actual HTTP client and credentials.",
            "config_fields":[
                {"name":"url","required":true,"value_type":"string | \"=expr\"","description":"The request URL."},
                {"name":"method","required":false,"value_type":"enum","description":"HTTP method.","enum_values":["GET","POST","PUT","PATCH","DELETE"]},
                {"name":"headers","required":false,"value_type":"object","description":"Request headers."},
                {"name":"body","required":false,"value_type":"JSON | \"=expr\"","description":"Optional request body."}
            ],
            "ports":{"inputs":["main"],"outputs":["main"]},
            "example":{"id":"http_request","name":"HTTP request","kind":"http_request","config":{"url":"https://example.com"}}
        })
    );
    assert!(tinyflows_bus::node_kinds::render_node_kinds_line(&contracts).contains("http_request"));
    Ok(())
}

#[tokio::test]
async fn apply_graph_ops_is_sequential_and_returns_no_partial_graph_on_failure()
-> tinybus::Result<()> {
    let (_server, client) = connections().await;
    let proxy = client.proxy(tinyflows_bus::BUS_NAME, catalog::OBJECT_PATH, catalog::INTERFACE)?;
    let request = ApplyGraphOpsRequest {
        graph: json!({"nodes":[{"id":"start","name":"Before","kind":"trigger"}],"edges":[]}),
        ops: vec![
            json!({"op":"set_node_name","id":"start","name":"After"}),
            json!({"op":"remove_node","id":"missing"}),
        ],
    };
    let result: catalog::ApplyGraphOpsResult = proxy
        .call(catalog::members::APPLY_GRAPH_OPS, (request,))
        .await?;
    assert!(result.graph.is_none());
    let error = result.error.unwrap();
    assert_eq!(error.index, 1);
    assert_eq!(error.op, "remove_node");
    assert!(error.details.contains("missing"));
    Ok(())
}

#[tokio::test]
async fn approval_manifest_uses_host_classification_and_preserves_candidate_order()
-> tinybus::Result<()> {
    let (_server, client) = connections().await;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let classes = vec![
        Some(ManifestClass {
            name: "Network".into(),
            blocked: false,
            skip: false,
        }),
        Some(ManifestClass {
            name: "Write".into(),
            blocked: false,
            skip: false,
        }),
        Some(ManifestClass {
            name: "Write".into(),
            blocked: false,
            skip: false,
        }),
        Some(ManifestClass {
            name: "Read".into(),
            blocked: false,
            skip: true,
        }),
        None,
        None,
    ];
    client
        .serve_at(
            "/test/approval".try_into()?,
            Classifier {
                classes,
                seen: seen.clone(),
            },
        )
        .await?;
    client.request_name("test.ApprovalHost").await?;
    let graph = json!({"nodes":[
        {"id":"http","name":"HTTP","kind":"http_request","config":{"url":"https://example.com"}},
        {"id":"code","name":"Code","kind":"code"},
        {"id":"connector","name":"Connector","kind":"tool_call","config":{"slug":"composio:GMAIL_SEND"}},
        {"id":"native","name":"Native","kind":"tool_call","config":{"slug":"oh:web_search"}},
        {"id":"dynamic","name":"Dynamic","kind":"tool_call","config":{"slug":"=item.tool"}},
        {"id":"agent","name":"Agent","kind":"agent","config":{"agent_ref":"agent-1"}}
    ],"edges":[]});
    let proxy = client.proxy(tinyflows_bus::BUS_NAME, catalog::OBJECT_PATH, catalog::INTERFACE)?;
    let entries: Vec<Value> = proxy
        .call(
            catalog::members::APPROVAL_MANIFEST,
            (ApprovalManifestRequest {
                graph,
                native_prefix: "oh:".into(),
                host: catalog::CallbackAddress {
                    destination: "test.ApprovalHost".into(),
                    object_path: "/test/approval".into(),
                },
            },),
        )
        .await?;
    let candidates = seen.lock().unwrap().clone();
    assert_eq!(candidates.len(), 6);
    assert!(matches!(candidates[0], ManifestCandidate::Http { .. }));
    assert!(matches!(
        candidates[3],
        ManifestCandidate::NativeTool { .. }
    ));
    assert_eq!(entries.len(), 4);
    assert_eq!(entries[0]["node_id"], "http");
    assert_eq!(entries[1]["node_id"], "code");
    assert_eq!(entries[2]["node_id"], "connector");
    assert_eq!(entries[3]["kind"], "dynamic");
    assert!(
        !entries
            .iter()
            .any(|entry| entry["tool_name"] == "web_search")
    );
    Ok(())
}

#[tokio::test]
async fn approval_manifest_fails_closed_on_malformed_classifier_reply_or_transport()
-> tinybus::Result<()> {
    let (_server, client) = connections().await;
    let seen = Arc::new(Mutex::new(Vec::new()));
    client
        .serve_at(
            "/test/approval".try_into()?,
            Classifier {
                classes: Vec::new(),
                seen,
            },
        )
        .await?;
    client.request_name("test.ApprovalHost").await?;
    let proxy = client.proxy(tinyflows_bus::BUS_NAME, catalog::OBJECT_PATH, catalog::INTERFACE)?;
    let request = ApprovalManifestRequest {
        graph: json!({"nodes":[{"id":"code","name":"Code","kind":"code"}],"edges":[]}),
        native_prefix: "oh:".into(),
        host: catalog::CallbackAddress {
            destination: "test.ApprovalHost".into(),
            object_path: "/test/approval".into(),
        },
    };
    let malformed: tinybus::Result<Vec<Value>> = proxy
        .call(catalog::members::APPROVAL_MANIFEST, (request.clone(),))
        .await;
    assert!(malformed.is_err());
    let unavailable = ApprovalManifestRequest {
        host: catalog::CallbackAddress {
            destination: "test.MissingApprovalHost".into(),
            object_path: "/missing".into(),
        },
        ..request
    };
    let transport: tinybus::Result<Vec<Value>> = proxy
        .call(catalog::members::APPROVAL_MANIFEST, (unavailable,))
        .await;
    assert!(transport.is_err());
    Ok(())
}

#[tokio::test]
async fn import_graph_normalizes_native_and_n8n_without_persisting() -> tinybus::Result<()> {
    let (_server, client) = connections().await;
    let proxy = client.proxy(
        tinyflows_bus::BUS_NAME,
        tinyflows_bus::OBJECT_PATH,
        tinyflows_bus::INTERFACE,
    )?;
    let native: tinyflows_bus::ImportGraphResult = proxy.call(tinyflows_bus::methods::IMPORT_GRAPH, (tinyflows_bus::ImportGraphRequest {
        graph: json!({"nodes":[{"id":"start","name":"Start","kind":"trigger","config":{"trigger_kind":"manual"}}],"edges":[]}),
        format: Some("native".into()),
    },)).await?;
    assert!(native.errors.is_empty(), "{:?}", native.errors);
    assert!(native.warnings.is_empty());
    assert!(native.graph.unwrap()["schema_version"].is_number());

    let n8n: tinyflows_bus::ImportGraphResult = proxy.call(tinyflows_bus::methods::IMPORT_GRAPH, (tinyflows_bus::ImportGraphRequest {
        graph: json!({"name":"imported","nodes":[{"id":"unknown","name":"Unknown","type":"custom.unknown"}],"connections":{}}),
        format: Some("auto".into()),
    },)).await?;
    assert!(n8n.errors.is_empty(), "{:?}", n8n.errors);
    assert!(!n8n.warnings.is_empty());
    assert!(n8n.graph.is_some());
    Ok(())
}
