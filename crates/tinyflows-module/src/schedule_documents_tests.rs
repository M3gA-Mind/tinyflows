use super::*;

use std::sync::Arc;
use tinystoragedrivers_core::{
    Capability, ErrorKind, MemoryStorage, Scope, StorageBackend, WriteOp,
};

struct Callback(Arc<dyn DocumentStore>);
#[tinybus::interface(name = "ai.tinyhumans.tinyflows.ScheduleDocuments")]
impl Callback {
    async fn document(&self, request: Value) -> tinybus::Result<Value> {
        let value = match request["op"].as_str().unwrap() {
            "capabilities" => Ok(
                json!({"transactions": self.0.capabilities().contains(Capability::Transactions)}),
            ),
            "atomic_batch" => self
                .0
                .atomic_batch(serde_json::from_value(request["ops"].clone()).unwrap())
                .await
                .map(|results| serde_json::to_value(results).unwrap()),
            _ => Err(StorageError::invalid_input("unexpected document request")),
        };
        Ok(match value {
            Ok(value) => json!({"value": value}),
            Err(error) => {
                json!({"error":{"kind":error.kind().to_string().replace(' ', "_"),"message":error.to_string()}})
            }
        })
    }
}

#[tokio::test]
async fn document_bridge_preserves_atomic_writes_conflicts_and_collection_scope() {
    let bus = tinybus::transport::memory::MemoryBus::new();
    tinybus::broker::Broker::new().spawn(bus.clone());
    let connection = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let scoped = MemoryStorage::new().for_scope(&Scope::local()).unwrap();
    let storage = scoped.documents().clone();
    storage
        .ensure_collection(&CollectionSpec::new("cron_jobs"))
        .await
        .unwrap();
    storage
        .ensure_collection(&CollectionSpec::new("other_domain"))
        .await
        .unwrap();
    connection
        .serve_at(
            "/test/Documents".try_into().unwrap(),
            Callback(storage.clone()),
        )
        .await
        .unwrap();
    connection.request_name("test.Documents").await.unwrap();
    let documents = Documents::new(
        connection,
        CallbackAddress {
            destination: "test.Documents".into(),
            object_path: "/test/Documents".into(),
        },
    )
    .await
    .unwrap();
    assert!(documents.capabilities().contains(Capability::Transactions));
    let put = |collection: &str, precondition| WriteOp::Put {
        collection: collection.into(),
        id: "job".into(),
        doc: json!({"enabled":true}),
        precondition,
    };
    documents
        .atomic_batch(vec![put("cron_jobs", Precondition::Absent)])
        .await
        .unwrap();
    assert!(storage.get("cron_jobs", "job").await.unwrap().is_some());
    let error = documents
        .atomic_batch(vec![put("cron_jobs", Precondition::Absent)])
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Conflict);
    let error = documents
        .atomic_batch(vec![put("other_domain", Precondition::Absent)])
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(storage.get("other_domain", "job").await.unwrap().is_none());
}
