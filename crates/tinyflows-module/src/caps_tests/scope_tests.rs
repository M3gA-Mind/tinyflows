use super::*;

use std::sync::Arc;
use tinyflows_bus::catalog::{
    self, CatalogDocumentCursor, CatalogDocumentPage, CatalogDocumentPrecondition,
    CatalogDocumentQuery, CatalogDocumentValue, CatalogStorageFailure, CatalogStorageReply,
    DocDeleteRequest, DocDeleteWhereRequest, DocEnsureRequest, DocGetRequest, DocPutRequest,
    DocQueryRequest,
};
use tinyflows_drivers::catalog::FlowCatalogDocuments;
use tinystoragedrivers_core::{DocumentStore, MemoryStorage, Scope, StorageBackend};

struct DocumentsHost(Arc<dyn DocumentStore>);

#[tinybus::interface(name = "ai.tinyhumans.tinyflows.CatalogStorage")]
impl DocumentsHost {
    async fn doc_ensure(&self, request: DocEnsureRequest) -> tinybus::Result<CatalogStorageReply> {
        Ok(reply(self.0.ensure_collection(&request.spec).await))
    }
    async fn doc_get(&self, request: DocGetRequest) -> tinybus::Result<CatalogStorageReply> {
        Ok(reply(self.0.get(&request.collection, &request.id).await))
    }
    async fn doc_put(&self, request: DocPutRequest) -> tinybus::Result<CatalogStorageReply> {
        Ok(reply(self.0.put(&request.collection, &request.id, request.doc, request.precondition).await))
    }
    async fn doc_delete(&self, request: DocDeleteRequest) -> tinybus::Result<CatalogStorageReply> {
        Ok(reply(self.0.delete(&request.collection, &request.id, request.precondition).await))
    }
    async fn doc_query(&self, request: DocQueryRequest) -> tinybus::Result<CatalogStorageReply> {
        Ok(reply(self.0.query(&request.collection, &request.query).await))
    }
    async fn doc_delete_where(&self, request: DocDeleteWhereRequest) -> tinybus::Result<CatalogStorageReply> {
        Ok(reply(self.0.delete_where(&request.collection, &request.filter).await))
    }
}

fn reply<T: serde::Serialize>(result: tinystoragedrivers_core::Result<T>) -> CatalogStorageReply {
    match result {
        Ok(value) => CatalogStorageReply::Value { value: serde_json::to_value(value).unwrap() },
        Err(error) => CatalogStorageReply::Error { error: CatalogStorageFailure {
            kind: error.kind().to_string().replace(' ', "_"),
            message: error.to_string(),
        } },
    }
}

async fn storage_callback(host: &Connection, owner: &str) -> (CatalogStorage, FlowCatalogDocuments) {
    let scope = Scope::new(owner).unwrap();
    let storage = MemoryStorage::new().for_scope(&scope).unwrap();
    let docs: Arc<dyn DocumentStore> = storage.documents().clone();
    let path = format!("/documents/{owner}");
    host.serve_at(path.parse().unwrap(), DocumentsHost(Arc::clone(&docs))).await.unwrap();
    (
        CatalogStorage { callback: catalog::CallbackAddress {
            destination: "test.CatalogStorage".into(),
            object_path: path,
        } },
        FlowCatalogDocuments::new(docs),
    )
}

#[tokio::test]
async fn module_builder_uses_scoped_flow_documents_for_state_and_catalog_resolution() {
    let (module, host) = support::memory_bus().await;
    host.request_name("test.CatalogStorage").await.unwrap();
    let (alice_storage, alice_catalog) = storage_callback(&host, "alice").await;
    let (bob_storage, bob_catalog) = storage_callback(&host, "bob").await;
    let child = alice_catalog.create_flow(
        "Child".into(),
        tinyflows::model::WorkflowGraph::default(),
        false,
        false,
    ).await.unwrap();

    let alice = CapsHostPorts::capabilities(
        module.clone(), support::caps_host("/caps/alice"),
        RunScope { flow_id: "parent-flow".into(), storage: alice_storage },
    ).await.unwrap();
    let bob = CapsHostPorts::capabilities(
        module, support::caps_host("/caps/bob"),
        RunScope { flow_id: "parent-flow".into(), storage: bob_storage },
    ).await.unwrap();

    alice.state.store("key", json!({"owner":"alice"})).await.unwrap();
    assert_eq!(alice.state.load("key").await.unwrap(), Some(json!({"owner":"alice"})));
    assert_eq!(alice_catalog.kv_get("flow:parent-flow", "key").await.unwrap(), Some(json!({"owner":"alice"})));
    assert_eq!(alice_catalog.kv_get("parent-flow", "key").await.unwrap(), None);
    assert_eq!(bob.state.load("key").await.unwrap(), None);

    let resolved = alice.resolver.resolve(&child.id).await.unwrap();
    assert_eq!(resolved, child.graph);
    assert!(bob_catalog.get_flow(&child.id).await.unwrap().is_none());
    assert!(bob.resolver.resolve(&child.id).await.is_err());
}
