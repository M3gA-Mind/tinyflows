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
    let proxy = client.proxy(
        tinyflows_bus::BUS_NAME,
        catalog::OBJECT_PATH,
        catalog::INTERFACE,
    )?;
    let contracts: Vec<tinyflows_bus::node_kinds::NodeKindContract> =
        proxy.call(catalog::members::NODE_KINDS, ()).await?;
    assert_eq!(contracts.len(), tinyflows_bus::node_kinds::NODE_KINDS.len());
    let code = contracts
        .iter()
        .find(|contract| contract.kind == "code")
        .unwrap();
    assert_eq!(code.config_fields.len(), 2);
    assert_eq!(code.ports.outputs, ["main"]);
    assert_eq!(code.example["kind"], "code");
    assert!(code.notes.is_empty());
    assert_eq!(
        serde_json::to_value(&code).unwrap(),
        json!({
            "kind":"code",
            "summary":"Run a sandboxed JavaScript or Python snippet.",
            "description":"config.language + config.source, run via the host's CodeRunner capability.",
            "config_fields":[
                {"name":"language","required":true,"value_type":"enum","description":"The runtime language.","enum_values":["javascript","python"]},
                {"name":"source","required":true,"value_type":"string","description":"The code to run."}
            ],
            "ports":{"inputs":["main"],"outputs":["main"]},
            "example":{"id":"shape","name":"Shape","kind":"code","config":{"language":"javascript","source":"return { total: item.a + item.b };"}}
        })
    );
    assert!(tinyflows_bus::node_kinds::render_node_kinds_line(&contracts).contains("http_request"));
    assert_eq!(
        tinyflows_bus::node_kinds::render_node_kinds_line(std::slice::from_ref(code)),
        "code [required config.language/config.source] — Run a sandboxed JavaScript or Python snippet."
    );
    assert_eq!(
        tinyflows_bus::node_kinds::render_node_kinds_required(std::slice::from_ref(code)),
        "code(config.language, config.source)"
    );
    assert_eq!(
        code.clone().with_note("host overlay").notes,
        ["host overlay"]
    );
    let unknown: tinybus::Result<Value> = proxy.call("UnknownCatalogMember", ()).await;
    assert!(unknown.is_err());
    Ok(())
}

#[tokio::test]
async fn bus_node_contracts_are_exhaustively_in_parity_with_engine_compatibility_api()
-> tinybus::Result<()> {
    let (_server, client) = connections().await;
    let proxy = client.proxy(
        tinyflows_bus::BUS_NAME,
        catalog::OBJECT_PATH,
        catalog::INTERFACE,
    )?;
    let bus_contracts: Vec<tinyflows_bus::node_kinds::NodeKindContract> =
        proxy.call(catalog::members::NODE_KINDS, ()).await?;
    let engine_contracts = tinyflows::catalog::all_contracts();
    assert_eq!(bus_contracts.len(), 22);
    assert_eq!(bus_contracts.len(), engine_contracts.len());
    for (bus, engine) in bus_contracts.iter().zip(&engine_contracts) {
        assert_eq!(bus.kind, engine.kind);
        assert_eq!(
            serde_json::to_value(bus).unwrap(),
            serde_json::to_value(engine).unwrap()
        );
        let note = format!("host overlay for {}", bus.kind);
        let bus_overlay = bus.clone().with_note(note.clone());
        let engine_overlay = engine.clone().with_note(note);
        assert_eq!(
            serde_json::to_value(&bus_overlay).unwrap(),
            serde_json::to_value(&engine_overlay).unwrap()
        );
        assert_eq!(
            tinyflows_bus::node_kinds::render_node_kinds_line(std::slice::from_ref(&bus_overlay)),
            tinyflows::catalog::render_node_kinds_line(std::slice::from_ref(&engine_overlay))
        );
        assert_eq!(
            tinyflows_bus::node_kinds::render_node_kinds_required(std::slice::from_ref(
                &bus_overlay
            )),
            tinyflows::catalog::render_node_kinds_required(std::slice::from_ref(&engine_overlay))
        );
    }
    let bus_line = tinyflows_bus::node_kinds::render_node_kinds_line(&bus_contracts);
    let engine_line = tinyflows::catalog::render_node_kinds_line(&engine_contracts);
    assert_eq!(bus_line, engine_line);
    assert_eq!(
        tinyflows_bus::node_kinds::render_node_kinds_required(&bus_contracts),
        tinyflows::catalog::render_node_kinds_required(&engine_contracts)
    );
    Ok(())
}

#[tokio::test]
async fn apply_graph_ops_is_sequential_and_returns_no_partial_graph_on_failure()
-> tinybus::Result<()> {
    let (_server, client) = connections().await;
    let proxy = client.proxy(
        tinyflows_bus::BUS_NAME,
        catalog::OBJECT_PATH,
        catalog::INTERFACE,
    )?;
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
            "/test/approval_missing".try_into()?,
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
    let proxy = client.proxy(
        tinyflows_bus::BUS_NAME,
        catalog::OBJECT_PATH,
        catalog::INTERFACE,
    )?;
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
    assert_eq!(entries.len(), 5);
    assert_eq!(entries[0]["node_id"], "http");
    assert_eq!(entries[1]["node_id"], "code");
    assert_eq!(entries[2]["node_id"], "connector");
    assert_eq!(entries[3]["kind"], "dynamic");
    assert_eq!(entries[4]["kind"], "agent");
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
    let proxy = client.proxy(
        tinyflows_bus::BUS_NAME,
        catalog::OBJECT_PATH,
        catalog::INTERFACE,
    )?;
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
    client
        .serve_at(
            "/test/approval".try_into()?,
            Classifier {
                classes: vec![None],
                seen: Arc::new(Mutex::new(Vec::new())),
            },
        )
        .await?;
    let missing_required_class: tinybus::Result<Vec<Value>> = proxy
        .call(
            catalog::members::APPROVAL_MANIFEST,
            (ApprovalManifestRequest {
                graph: json!({"nodes":[{"id":"code","name":"Code","kind":"code"}],"edges":[]}),
                native_prefix: "oh:".into(),
                host: catalog::CallbackAddress {
                    destination: "test.ApprovalHost".into(),
                    object_path: "/test/approval_missing".into(),
                },
            },),
        )
        .await;
    assert!(missing_required_class.is_err());
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
            format: Some(" TinyFlows ".into()),
    },)).await?;
    assert!(native.errors.is_empty(), "{:?}", native.errors);
    assert!(native.warnings.is_empty());
    assert!(native.graph.unwrap()["schema_version"].is_number());

    let auto_native: tinyflows_bus::ImportGraphResult = proxy
        .call(
            tinyflows_bus::methods::IMPORT_GRAPH,
            (tinyflows_bus::ImportGraphRequest {
                graph: json!({"nodes":[{"id":"start","name":"Start","kind":"trigger","config":{"trigger_kind":"manual"}}],"edges":[]}),
                format: Some("".into()),
            },),
        )
        .await?;
    assert!(auto_native.errors.is_empty(), "{:?}", auto_native.errors);

    let n8n: tinyflows_bus::ImportGraphResult = proxy.call(tinyflows_bus::methods::IMPORT_GRAPH, (tinyflows_bus::ImportGraphRequest {
        graph: json!({"name":"imported","nodes":[{"id":"unknown","name":"Unknown","type":"custom.unknown"}],"connections":{}}),
            format: Some(" N8N ".into()),
    },)).await?;
    assert!(n8n.errors.is_empty(), "{:?}", n8n.errors);
    assert!(!n8n.warnings.is_empty());
    assert!(n8n.graph.is_some());
    Ok(())
}
