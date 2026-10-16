//! Fixtures the store tests share.

use std::sync::Arc;

use serde_json::json;
use tinyflows::store::parse_workflow;
use tinyflows::store::types::WorkflowRecord;
use tinystoragedrivers_core::{DocumentStore, MemoryStorage, Scope, StorageBackend};

use super::DriverWorkflowStore;

/// `storage`'s documents under `scope`.
pub(crate) fn docs_in(storage: &MemoryStorage, scope: &str) -> Arc<dyn DocumentStore> {
    Arc::clone(
        storage
            .for_scope(&Scope::new(scope).unwrap())
            .unwrap()
            .documents(),
    )
}

/// A store over a fresh in-memory backend.
pub(crate) fn store() -> DriverWorkflowStore {
    DriverWorkflowStore::new(docs_in(&MemoryStorage::new(), "local"))
}

/// A second store instance over the same storage as `first`, as a daemon task
/// or another process would open.
pub(crate) fn store_over(storage: &MemoryStorage, scope: &str) -> DriverWorkflowStore {
    DriverWorkflowStore::new(docs_in(storage, scope))
}

/// The smallest document that validates: one trigger, one transform, one edge.
pub(crate) fn valid_document(id: &str) -> String {
    json!({
        "id": id,
        "name": "Greet",
        "description": "says hello",
        "nodes": [
            { "id": "t", "kind": "trigger", "name": "start",
              "config": { "trigger_kind": "manual" } },
            { "id": "greet", "kind": "transform", "name": "greet",
              "config": { "set": { "greeting": "=.item.name" } } }
        ],
        "edges": [
            { "from_node": "t", "from_port": "main", "to_node": "greet", "to_port": "main" }
        ]
    })
    .to_string()
}

/// A valid record with `id` and `description`.
pub(crate) fn record(id: &str, description: &str) -> WorkflowRecord {
    let mut record = parse_workflow(&valid_document(id), id).expect("a valid document");
    record.description = description.to_string();
    record
}

/// Runs a storage future from a synchronous test.
pub(crate) fn block_on<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(future)
}

/// A pass-through handle whose writes to one collection can be made to fail,
/// or to fail only when a precondition is a version, so a test can drive the
/// paths a healthy backend never takes.
#[derive(Debug)]
pub(crate) struct Faulty {
    inner: Arc<dyn DocumentStore>,
    collection: &'static str,
    pub(crate) fail_puts: std::sync::atomic::AtomicBool,
    pub(crate) fail_batches: std::sync::atomic::AtomicBool,
    pub(crate) transactions: bool,
}

impl Faulty {
    /// Wraps `inner`, watching puts to `collection`.
    pub(crate) fn wrap(inner: Arc<dyn DocumentStore>, collection: &'static str) -> Arc<Self> {
        Self::wrap_with(inner, collection, true)
    }

    /// [`Self::wrap`], advertising transactions only if `transactions`.
    pub(crate) fn wrap_with(
        inner: Arc<dyn DocumentStore>,
        collection: &'static str,
        transactions: bool,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner,
            collection,
            fail_puts: false.into(),
            fail_batches: false.into(),
            transactions,
        })
    }
}

#[tinystoragedrivers_core::async_trait]
impl DocumentStore for Faulty {
    fn capabilities(&self) -> tinystoragedrivers_core::Capabilities {
        if self.transactions {
            self.inner.capabilities()
        } else {
            self.inner
                .capabilities()
                .without(tinystoragedrivers_core::Capability::Transactions)
                .without(tinystoragedrivers_core::Capability::ValidatedBatch)
        }
    }

    async fn ensure_collection(
        &self,
        spec: &tinystoragedrivers_core::CollectionSpec,
    ) -> tinystoragedrivers_core::Result<()> {
        self.inner.ensure_collection(spec).await
    }

    async fn get(
        &self,
        collection: &str,
        id: &str,
    ) -> tinystoragedrivers_core::Result<
        Option<tinystoragedrivers_core::Versioned<serde_json::Value>>,
    > {
        self.inner.get(collection, id).await
    }

    async fn put(
        &self,
        collection: &str,
        id: &str,
        doc: serde_json::Value,
        precondition: tinystoragedrivers_core::Precondition,
    ) -> tinystoragedrivers_core::Result<tinystoragedrivers_core::Version> {
        if collection == self.collection && self.fail_puts.load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(tinystoragedrivers_core::StorageError::backend(
                "injected write failure",
            ));
        }
        self.inner.put(collection, id, doc, precondition).await
    }

    async fn put_with(
        &self,
        collection: &str,
        id: &str,
        doc: serde_json::Value,
        precondition: tinystoragedrivers_core::Precondition,
        options: tinystoragedrivers_core::WriteOptions,
    ) -> tinystoragedrivers_core::Result<tinystoragedrivers_core::Version> {
        if collection == self.collection && self.fail_puts.load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(tinystoragedrivers_core::StorageError::backend(
                "injected write failure",
            ));
        }
        self.inner
            .put_with(collection, id, doc, precondition, options)
            .await
    }

    async fn delete(
        &self,
        collection: &str,
        id: &str,
        precondition: tinystoragedrivers_core::Precondition,
    ) -> tinystoragedrivers_core::Result<bool> {
        self.inner.delete(collection, id, precondition).await
    }

    async fn query(
        &self,
        collection: &str,
        query: &tinystoragedrivers_core::Query,
    ) -> tinystoragedrivers_core::Result<
        tinystoragedrivers_core::Page<tinystoragedrivers_core::Versioned<serde_json::Value>>,
    > {
        self.inner.query(collection, query).await
    }

    async fn count(
        &self,
        collection: &str,
        filter: &tinystoragedrivers_core::Filter,
    ) -> tinystoragedrivers_core::Result<u64> {
        self.inner.count(collection, filter).await
    }

    async fn delete_where(
        &self,
        collection: &str,
        filter: &tinystoragedrivers_core::Filter,
    ) -> tinystoragedrivers_core::Result<u64> {
        self.inner.delete_where(collection, filter).await
    }

    async fn claim(
        &self,
        collection: &str,
        filter: &tinystoragedrivers_core::Filter,
        sort: &[tinystoragedrivers_core::Sort],
        patch: &serde_json::Value,
    ) -> tinystoragedrivers_core::Result<
        Option<tinystoragedrivers_core::Versioned<serde_json::Value>>,
    > {
        self.inner.claim(collection, filter, sort, patch).await
    }

    async fn drop_collection(&self, collection: &str) -> tinystoragedrivers_core::Result<()> {
        self.inner.drop_collection(collection).await
    }

    async fn atomic_batch(
        &self,
        ops: Vec<tinystoragedrivers_core::WriteOp>,
    ) -> tinystoragedrivers_core::Result<Vec<tinystoragedrivers_core::WriteResult>> {
        if self.fail_batches.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(tinystoragedrivers_core::StorageError::backend(
                "injected batch failure",
            ));
        }
        self.inner.atomic_batch(ops).await
    }

    async fn atomic_batch_checked(
        &self,
        checks: Vec<tinystoragedrivers_core::ReadCheck>,
        ops: Vec<tinystoragedrivers_core::WriteOp>,
    ) -> tinystoragedrivers_core::Result<Vec<tinystoragedrivers_core::WriteResult>> {
        if self.fail_batches.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(tinystoragedrivers_core::StorageError::backend(
                "injected batch failure",
            ));
        }
        self.inner.atomic_batch_checked(checks, ops).await
    }
}
