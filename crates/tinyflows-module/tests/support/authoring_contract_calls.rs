//! Calls added on the Authoring and Catalog contracts through the compiled module.

use serde_json::{Value, json};
use tinybus::{Connection, Proxy};
use tinyflows_bus::catalog::{self, ManifestCandidate, ManifestClass};

pub async fn verify_surface(proxy: &Proxy, catalog_proxy: &Proxy) -> Value {
    let report: tinyflows_bus::GraphValidation = proxy
        .call(
            tinyflows_bus::methods::VALIDATE_GRAPH,
            (tinyflows_bus::ValidateGraphRequest {
                graph: json!({
                    "nodes": [{"id":"start", "name":"Start", "kind":"trigger", "config":{}}],
                    "edges": []
                }),
                host: None,
            },),
        )
        .await
        .unwrap();
    assert!(report.is_valid(), "{:?}", report.errors);
    let graph = report.graph.unwrap();
    assert!(graph["schema_version"].as_u64().unwrap() > 0);

    let contracts: Vec<tinyflows_bus::node_kinds::NodeKindContract> = catalog_proxy
        .call(catalog::members::NODE_KINDS, ())
        .await
        .unwrap();
    assert_eq!(contracts.len(), 22);
    let applied: catalog::ApplyGraphOpsResult = catalog_proxy
        .call(
            catalog::members::APPLY_GRAPH_OPS,
            (catalog::ApplyGraphOpsRequest {
                graph: graph.clone(),
                ops: vec![json!({"op":"set_node_name","id":"start","name":"Edited"})],
            },),
        )
        .await
        .unwrap();
    assert_eq!(applied.graph.unwrap()["nodes"][0]["name"], "Edited");

    let imported: tinyflows_bus::ImportGraphResult = proxy
        .call(
            tinyflows_bus::methods::IMPORT_GRAPH,
            (tinyflows_bus::ImportGraphRequest {
                graph: graph.clone(),
                format: Some("native".into()),
            },),
        )
        .await
        .unwrap();
    assert!(imported.errors.is_empty(), "{:?}", imported.errors);
    assert!(imported.graph.is_some());
    graph
}

struct ApprovalClassifier;

#[tinybus::interface(name = "ai.tinyhumans.tinyflows.ApprovalManifestHost")]
impl ApprovalClassifier {
    async fn classify_manifest_candidates(
        &self,
        request: catalog::ClassifyManifestCandidatesRequest,
    ) -> tinybus::Result<Vec<Option<ManifestClass>>> {
        Ok(request
            .candidates
            .iter()
            .map(|candidate| match candidate {
                ManifestCandidate::DynamicTool { .. } | ManifestCandidate::Agent { .. } => None,
                _ => Some(ManifestClass {
                    name: "Write".into(),
                    blocked: false,
                    skip: false,
                }),
            })
            .collect())
    }
}

pub async fn approval_manifest(connection: &Connection, proxy: &Proxy) -> Vec<Value> {
    connection
        .serve_at(
            "/approval_manifest_host".try_into().unwrap(),
            ApprovalClassifier,
        )
        .await
        .unwrap();
    connection
        .request_name("test.ApprovalManifestHost")
        .await
        .unwrap();
    proxy
        .call(
            catalog::members::APPROVAL_MANIFEST,
            (catalog::ApprovalManifestRequest {
                graph: json!({
                    "nodes":[{"id":"code","name":"Code","kind":"code"}],
                    "edges":[]
                }),
                native_prefix: "oh:".into(),
                host: catalog::CallbackAddress {
                    destination: "test.ApprovalManifestHost".into(),
                    object_path: "/approval_manifest_host".into(),
                },
            },),
        )
        .await
        .unwrap()
}
