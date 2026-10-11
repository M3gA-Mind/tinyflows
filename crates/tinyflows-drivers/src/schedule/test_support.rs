//! Fixtures the schedule tests share.

use std::sync::Arc;

use tinyflows_schedule::{JobOrigin, Schedule};
use tinystoragedrivers_core::{DocumentStore, MemoryStorage, Scope, StorageBackend};

use super::CronDocuments;

/// `storage`'s documents under `scope`.
pub(super) fn docs_in(storage: &MemoryStorage, scope: &str) -> Arc<dyn DocumentStore> {
    Arc::clone(
        storage
            .for_scope(&Scope::new(scope).unwrap())
            .unwrap()
            .documents(),
    )
}

/// A store over a fresh in-memory backend.
pub(super) fn store() -> CronDocuments {
    CronDocuments::new(docs_in(&MemoryStorage::new(), "local"))
}

/// Every day at 09:00 UTC.
pub(super) fn daily() -> Schedule {
    Schedule::Cron {
        expr: "0 9 * * *".into(),
        tz: None,
        active_hours: None,
    }
}

pub(super) fn channel_origin() -> JobOrigin {
    JobOrigin::Channel {
        channel: "telegram".into(),
        reply_target: "chat-42".into(),
        history_key: "telegram:chat-42".into(),
        sender: Some("alice".into()),
        thread_id: None,
    }
}

/// A forwarding driver with controlled capability and transaction failures.
#[derive(Debug)]
pub(super) struct FaultDocuments {
    pub inner: Arc<dyn DocumentStore>,
    pub transactions: bool,
    pub fail_batch: std::sync::atomic::AtomicBool,
    pub batch_barrier: std::sync::Mutex<Option<Arc<tokio::sync::Barrier>>>,
}
#[async_trait::async_trait]
impl DocumentStore for FaultDocuments {
    fn capabilities(&self) -> tinystoragedrivers_core::Capabilities {
        if self.transactions {
            self.inner.capabilities()
        } else {
            self.inner
                .capabilities()
                .without(tinystoragedrivers_core::Capability::Transactions)
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
        c: &str,
        id: &str,
    ) -> tinystoragedrivers_core::Result<
        Option<tinystoragedrivers_core::Versioned<serde_json::Value>>,
    > {
        self.inner.get(c, id).await
    }
    async fn put(
        &self,
        c: &str,
        id: &str,
        doc: serde_json::Value,
        p: tinystoragedrivers_core::Precondition,
    ) -> tinystoragedrivers_core::Result<tinystoragedrivers_core::Version> {
        self.inner.put(c, id, doc, p).await
    }
    async fn delete(
        &self,
        c: &str,
        id: &str,
        p: tinystoragedrivers_core::Precondition,
    ) -> tinystoragedrivers_core::Result<bool> {
        self.inner.delete(c, id, p).await
    }
    async fn query(
        &self,
        c: &str,
        q: &tinystoragedrivers_core::Query,
    ) -> tinystoragedrivers_core::Result<
        tinystoragedrivers_core::Page<tinystoragedrivers_core::Versioned<serde_json::Value>>,
    > {
        self.inner.query(c, q).await
    }
    async fn count(
        &self,
        c: &str,
        f: &tinystoragedrivers_core::Filter,
    ) -> tinystoragedrivers_core::Result<u64> {
        self.inner.count(c, f).await
    }
    async fn delete_where(
        &self,
        c: &str,
        f: &tinystoragedrivers_core::Filter,
    ) -> tinystoragedrivers_core::Result<u64> {
        self.inner.delete_where(c, f).await
    }
    async fn claim(
        &self,
        c: &str,
        f: &tinystoragedrivers_core::Filter,
        s: &[tinystoragedrivers_core::Sort],
        p: &serde_json::Value,
    ) -> tinystoragedrivers_core::Result<
        Option<tinystoragedrivers_core::Versioned<serde_json::Value>>,
    > {
        self.inner.claim(c, f, s, p).await
    }
    async fn drop_collection(&self, c: &str) -> tinystoragedrivers_core::Result<()> {
        self.inner.drop_collection(c).await
    }
    async fn atomic_batch(
        &self,
        ops: Vec<tinystoragedrivers_core::WriteOp>,
    ) -> tinystoragedrivers_core::Result<Vec<tinystoragedrivers_core::WriteResult>> {
        let barrier = self.batch_barrier.lock().unwrap().take();
        if let Some(barrier) = barrier {
            barrier.wait().await;
        }
        if self.fail_batch.load(std::sync::atomic::Ordering::SeqCst) || !self.transactions {
            return Err(tinystoragedrivers_core::StorageError::unsupported(
                tinystoragedrivers_core::Capability::Transactions,
                "fixture refuses transaction",
            ));
        }
        self.inner.atomic_batch(ops).await
    }
}
