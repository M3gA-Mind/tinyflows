use super::CronDocuments;
use async_trait::async_trait;
use chrono::{Duration, Utc};
use serde_json::Value;
use std::sync::Arc;
use tinyflows_schedule::Schedule;
use tokio::sync::Notify;
use tinystoragedrivers_core::{
    Capabilities, CollectionSpec, DocumentStore, Filter, MemoryStorage, Page, Precondition, Query,
    Scope, Sort, StorageBackend, Version, Versioned, WriteOp, WriteResult,
};

#[derive(Debug)]
struct HeldClaimWrite {
    inner: Arc<dyn DocumentStore>,
    entered: Notify,
    release: Notify,
}

#[async_trait]
impl DocumentStore for HeldClaimWrite {
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }

    async fn ensure_collection(&self, spec: &CollectionSpec) -> tinystoragedrivers_core::Result<()> {
        self.inner.ensure_collection(spec).await
    }

    async fn get(
        &self,
        collection: &str,
        id: &str,
    ) -> tinystoragedrivers_core::Result<Option<Versioned<Value>>> {
        self.inner.get(collection, id).await
    }

    async fn put(
        &self,
        collection: &str,
        id: &str,
        doc: Value,
        precondition: Precondition,
    ) -> tinystoragedrivers_core::Result<Version> {
        self.inner.put(collection, id, doc, precondition).await
    }

    async fn delete(
        &self,
        collection: &str,
        id: &str,
        precondition: Precondition,
    ) -> tinystoragedrivers_core::Result<bool> {
        self.inner.delete(collection, id, precondition).await
    }

    async fn query(
        &self,
        collection: &str,
        query: &Query,
    ) -> tinystoragedrivers_core::Result<Page<Versioned<Value>>> {
        self.inner.query(collection, query).await
    }

    async fn count(
        &self,
        collection: &str,
        filter: &Filter,
    ) -> tinystoragedrivers_core::Result<u64> {
        self.inner.count(collection, filter).await
    }

    async fn delete_where(
        &self,
        collection: &str,
        filter: &Filter,
    ) -> tinystoragedrivers_core::Result<u64> {
        self.inner.delete_where(collection, filter).await
    }

    async fn claim(
        &self,
        collection: &str,
        filter: &Filter,
        sort: &[Sort],
        patch: &Value,
    ) -> tinystoragedrivers_core::Result<Option<Versioned<Value>>> {
        self.inner.claim(collection, filter, sort, patch).await
    }

    async fn atomic_batch(
        &self,
        ops: Vec<WriteOp>,
    ) -> tinystoragedrivers_core::Result<Vec<WriteResult>> {
        let late_claim = ops.iter().any(|op| match op {
            WriteOp::Put { collection, doc, .. } => {
                collection == "cron_jobs" && doc["occurrence_lease"]["owner"] == "late-owner"
            }
            _ => false,
        });
        if late_claim {
            self.entered.notify_one();
            self.release.notified().await;
        }
        self.inner.atomic_batch(ops).await
    }

    async fn drop_collection(&self, collection: &str) -> tinystoragedrivers_core::Result<()> {
        self.inner.drop_collection(collection).await
    }
}

#[tokio::test]
async fn admission_fence_rejects_claim_transaction_that_commits_after_reconciliation() {
    let backend = MemoryStorage::new();
    let scoped = backend.for_scope(&Scope::local()).unwrap();
    let held_docs = Arc::new(HeldClaimWrite {
        inner: scoped.documents().clone(),
        entered: Notify::new(),
        release: Notify::new(),
    });
    let store = CronDocuments::new(held_docs.clone());
    let now = Utc::now();
    let job = store
        .add_shell_job(
            None,
            Schedule::At {
                at: now + Duration::hours(1),
            },
            "echo fenced admission",
        )
        .await
        .unwrap();

    let claim_store = store.clone();
    let claim_job = job.clone();
    let late_claim = tokio::spawn(async move {
        claim_store
            .claim_job_now(&claim_job, "late-owner", now, Duration::hours(2))
            .await
    });
    held_docs.entered.notified().await;

    assert!(store
        .reconcile_manual_claim_admission(&job, "late-owner", now)
        .await
        .unwrap()
        .is_none());
    held_docs.release.notify_one();
    assert!(late_claim.await.unwrap().unwrap().is_none());

    let current = store.get_job(&job.id).await.unwrap();
    assert_eq!(current.claim_generation, job.claim_generation + 1);
    assert!(store
        .claim_job_now(&job, "late-owner", now, Duration::hours(2))
        .await
        .unwrap()
        .is_none(), "the stale job snapshot cannot revive the delayed claim");
    let fresh_claim = store
        .claim_job_now(&current, "fresh-owner", now, Duration::hours(2))
        .await
        .unwrap()
        .expect("a fresh snapshot remains admissible after the fence");
    assert!(store.release_claim(&fresh_claim, now).await.unwrap());
}
