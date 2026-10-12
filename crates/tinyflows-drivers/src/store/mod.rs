//! [`WorkflowStore`] on a `tinystoragedrivers` document handle.
//!
//! [`DriverWorkflowStore`] is the storage behind the engine's authoring layer
//! (`tinyflows::store`): saved workflow definitions, their superseded
//! revisions, run records, the per-workflow notes journal and proposed graph
//! changes. A host that opened a storage backend (SQLite on a desktop, MongoDB
//! in the cloud, memory in tests) hands it a document handle, usually one bound
//! to a tenant scope.
//!
//! # Layout
//!
//! | Collection | Document id | Holds |
//! | --- | --- | --- |
//! | `wf_definitions` | workflow id | one [`WorkflowRecord`] |
//! | `wf_revisions` | revision id | one superseded copy of a workflow |
//! | `wf_runs` | run id | one [`RunRecord`] |
//! | `wf_notes` | workflow id | the workflow's whole journal (at most [`MAX_NOTES`]) |
//! | `wf_proposals` | proposal id | one [`WorkflowProposal`] |
//! | `wf_locks` | workflow id | the lease behind [`WorkflowStore::lock_proposal_decision`] |
//! | `wf_quarantine` | random token | a journal that could not be parsed, set aside |
//!
//! A record's nested data (a graph's free-form node config, a proposal's ops)
//! is stored as a JSON **string** beside the few scalar fields queries use, so
//! a MongoDB-backed port never meets an arbitrary key. Ids longer than the
//! drivers' key limit are replaced by their SHA-256 in the document id; the
//! record itself keeps the real id.
//!
//! # What the port gives in place of files
//!
//! The file store serialised writers with advisory file locks. The port has
//! single-document compare-and-swap instead, so:
//!
//! - every definition write is a compare-and-swap on the version it read. A
//!   save or delete first writes the revision of what it is about to replace,
//!   then swaps the definition, and takes the revision back when the swap loses
//!   (then retries). A crash between the two leaves one extra revision, never
//!   a lost one;
//! - [`WorkflowStore::save_if_fingerprint`] and
//!   [`WorkflowStore::save_if_record_fingerprint`] compare the fingerprint on
//!   the document they swap, so two processes (or store instances) racing the
//!   same edit cannot both win;
//! - the journal is one document per workflow, updated by compare-and-swap;
//! - [`WorkflowStore::lock_proposal_decision`] is a lease document, claimed
//!   with a write-if-absent (or a swap over an expired lease), so it holds
//!   across processes sharing one database. The lease lasts [`LOCK_LEASE`]; a
//!   decision that takes longer than that loses its exclusion.
//!
//! The trait is synchronous, so each call runs on `tinystoragedrivers`'
//! [`Blocking`] bridge (one per process, or the host's own through
//! [`DriverWorkflowStore::with_bridge`]), which is safe from inside any runtime.

use std::sync::{Arc, OnceLock};

use tinyflows::store::types::{
    RunRecord, WorkflowError, WorkflowNote, WorkflowProposal, WorkflowRecord, WorkflowRevision,
    WorkflowSummary,
};
use tinyflows::store::{
    EnginePolicy, HostPolicy, MAX_NOTES, ProposalDecisionGuard, WorkflowStore,
};
use tinystoragedrivers_core::{
    Blocking, CollectionSpec, DocumentStore, IndexSpec, StorageError,
};
use tokio::sync::OnceCell;

mod definitions;
mod history;
mod locks;
mod notes;
mod proposals;
mod runs;

pub use locks::LOCK_LEASE;

pub(crate) const DEFINITIONS: &str = "wf_definitions";
pub(crate) const REVISIONS: &str = "wf_revisions";
pub(crate) const RUNS: &str = "wf_runs";
pub(crate) const NOTES: &str = "wf_notes";
pub(crate) const PROPOSALS: &str = "wf_proposals";
pub(crate) const LOCKS: &str = "wf_locks";
pub(crate) const QUARANTINE: &str = "wf_quarantine";

/// Compare-and-swap attempts before a contended write gives up.
pub(crate) const CAS_ATTEMPTS: usize = 32;

/// Longest id stored as is; longer ones are hashed.
const MAX_ID_LEN: usize = 400;

/// What the async halves share.
pub(crate) struct Inner {
    pub(crate) docs: Arc<dyn DocumentStore>,
    pub(crate) policy: Arc<dyn HostPolicy>,
    declared: Arc<OnceCell<()>>,
}

/// The workflow store on a document handle. Cheap to clone; clones share the
/// handle, the policy and the bridge.
#[derive(Clone)]
pub struct DriverWorkflowStore {
    inner: Arc<Inner>,
    bridge: Option<Arc<Blocking>>,
    scope_label: String,
}

impl std::fmt::Debug for DriverWorkflowStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DriverWorkflowStore")
            .field("scope_label", &self.scope_label)
            .finish_non_exhaustive()
    }
}

impl DriverWorkflowStore {
    /// The store in `docs`, judging documents by the engine's own rules.
    pub fn new(docs: Arc<dyn DocumentStore>) -> Self {
        Self {
            inner: Arc::new(Inner {
                docs,
                policy: Arc::new(EnginePolicy),
                declared: Arc::new(OnceCell::new()),
            }),
            bridge: None,
            scope_label: "documents".to_string(),
        }
    }

    /// The same store, judging every document and every edit by `policy`.
    ///
    /// Applied to reads as well as writes, so a document that became invalid
    /// because the host's vocabulary changed under it is reported the next time
    /// it is read rather than silently running somewhere else.
    #[must_use]
    pub fn with_policy(self, policy: Arc<dyn HostPolicy>) -> Self {
        Self {
            inner: Arc::new(Inner {
                docs: Arc::clone(&self.inner.docs),
                policy,
                declared: Arc::clone(&self.inner.declared),
            }),
            ..self
        }
    }

    /// The same store, blocking on `bridge` rather than the process-wide one.
    #[must_use]
    pub fn with_bridge(self, bridge: Arc<Blocking>) -> Self {
        Self {
            bridge: Some(bridge),
            ..self
        }
    }

    /// The same store, naming itself `label` in
    /// [`WorkflowStore::proposal_decision_scope`]. Instances over the same
    /// storage should share a label; unrelated catalogs in one process should
    /// not.
    #[must_use]
    pub fn with_scope_label(self, label: impl Into<String>) -> Self {
        Self {
            scope_label: label.into(),
            ..self
        }
    }

    /// Every collection the store uses, with its indexes.
    pub fn collections() -> Vec<CollectionSpec> {
        vec![
            CollectionSpec::new(DEFINITIONS),
            CollectionSpec::new(REVISIONS)
                .index(IndexSpec::new("by_workflow", ["workflow_id", "superseded_at"])),
            CollectionSpec::new(RUNS)
                .index(IndexSpec::new("by_workflow", ["workflow_id", "started_at"]))
                .index(IndexSpec::new("by_settled", ["settled", "started_at"])),
            CollectionSpec::new(NOTES),
            CollectionSpec::new(PROPOSALS)
                .index(IndexSpec::new("by_workflow", ["workflow_id", "created_at"])),
            CollectionSpec::new(LOCKS),
            CollectionSpec::new(QUARANTINE),
        ]
    }

    /// Declares [`Self::collections`]. Every method does this on first use; a
    /// host may call it at startup to fail early.
    ///
    /// # Errors
    ///
    /// When the backend refuses a collection or index.
    pub fn ensure(&self) -> Result<(), WorkflowError> {
        let inner = Arc::clone(&self.inner);
        self.block(async move { inner.ensure().await })
    }

    /// Runs `future` to completion on the bridge.
    pub(crate) fn block<T, F>(&self, future: F) -> Result<T, WorkflowError>
    where
        F: std::future::Future<Output = Result<T, WorkflowError>> + Send + 'static,
        T: Send + 'static,
    {
        let ran = match &self.bridge {
            Some(bridge) => bridge.run(future),
            None => shared_bridge()?.run(future),
        };
        ran.map_err(storage)?
    }
}

fn shared_bridge() -> Result<&'static Blocking, WorkflowError> {
    static BRIDGE: OnceLock<Result<Blocking, String>> = OnceLock::new();
    BRIDGE
        .get_or_init(|| Blocking::new().map_err(|error| error.to_string()))
        .as_ref()
        .map_err(|error| WorkflowError::Storage(error.clone()))
}

impl Inner {
    pub(crate) async fn ensure(&self) -> Result<(), WorkflowError> {
        self.declared
            .get_or_try_init(|| async {
                for spec in DriverWorkflowStore::collections() {
                    self.docs
                        .ensure_collection(&spec)
                        .await
                        .map_err(storage)?;
                }
                Ok::<(), WorkflowError>(())
            })
            .await
            .map(|_| ())
    }
}

/// A storage failure as the store's error.
pub(crate) fn storage(error: StorageError) -> WorkflowError {
    WorkflowError::Storage(error.to_string())
}

/// A (de)serialization failure as the store's error.
pub(crate) fn malformed(context: &str, error: impl std::fmt::Display) -> WorkflowError {
    WorkflowError::Malformed(format!("{context}: {error}"))
}

/// The document id for an id the caller chose: refused when empty or
/// containing a control character, hashed when too long to use as a key.
pub(crate) fn doc_id(id: &str) -> Result<String, WorkflowError> {
    if id.is_empty() || id.chars().any(char::is_control) {
        return Err(WorkflowError::Malformed(format!(
            "{id:?} is not a usable id: it is empty or contains a control character"
        )));
    }
    if id.len() <= MAX_ID_LEN {
        return Ok(id.to_string());
    }
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(id.as_bytes());
    Ok(format!("h:{}", hex_lower(&digest)))
}

fn hex_lower(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Epoch milliseconds, saturating at zero if the clock is before the epoch.
pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or_default()
}

/// A stamp as the signed integer documents order and filter on.
pub(crate) fn stamp(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

impl WorkflowStore for DriverWorkflowStore {
    fn policy(&self) -> &dyn HostPolicy {
        self.inner.policy.as_ref()
    }

    fn proposal_decision_scope(&self) -> String {
        format!("documents:{}", self.scope_label)
    }

    fn list(&self) -> Result<Vec<WorkflowSummary>, WorkflowError> {
        let inner = Arc::clone(&self.inner);
        self.block(async move { inner.list().await })
    }

    fn get(&self, id: &str) -> Result<Option<WorkflowRecord>, WorkflowError> {
        let (inner, id) = (Arc::clone(&self.inner), id.to_string());
        self.block(async move { inner.get(&id).await })
    }

    fn save(&self, record: &WorkflowRecord) -> Result<(), WorkflowError> {
        let (inner, record) = (Arc::clone(&self.inner), record.clone());
        self.block(async move { inner.save(&record).await })
    }

    fn save_if_fingerprint(
        &self,
        record: &WorkflowRecord,
        expected_fingerprint: &str,
    ) -> Result<bool, WorkflowError> {
        let (inner, record, expected) = (
            Arc::clone(&self.inner),
            record.clone(),
            expected_fingerprint.to_string(),
        );
        self.block(async move {
            inner
                .save_if_current(&record, &expected, |current| {
                    tinyflows::store::types::fingerprint(&current.graph)
                })
                .await
        })
    }

    fn save_if_record_fingerprint(
        &self,
        record: &WorkflowRecord,
        expected_fingerprint: &str,
    ) -> Result<bool, WorkflowError> {
        let (inner, record, expected) = (
            Arc::clone(&self.inner),
            record.clone(),
            expected_fingerprint.to_string(),
        );
        self.block(async move {
            inner
                .save_if_current(
                    &record,
                    &expected,
                    tinyflows::store::types::record_fingerprint,
                )
                .await
        })
    }

    fn delete(&self, id: &str) -> Result<(), WorkflowError> {
        let (inner, id) = (Arc::clone(&self.inner), id.to_string());
        self.block(async move { inner.delete(&id).await })
    }

    fn record_run(&self, run: &RunRecord) -> Result<(), WorkflowError> {
        let (inner, run) = (Arc::clone(&self.inner), run.clone());
        self.block(async move { inner.record_run(&run).await })
    }

    fn get_run(&self, run_id: &str) -> Result<Option<RunRecord>, WorkflowError> {
        let (inner, id) = (Arc::clone(&self.inner), run_id.to_string());
        self.block(async move { inner.get_run(&id).await })
    }

    fn list_runs(&self, workflow_id: &str) -> Result<Vec<RunRecord>, WorkflowError> {
        let (inner, id) = (Arc::clone(&self.inner), workflow_id.to_string());
        self.block(async move { inner.list_runs(&id).await })
    }

    fn unsettled_runs(&self) -> Result<Vec<RunRecord>, WorkflowError> {
        let inner = Arc::clone(&self.inner);
        self.block(async move { inner.unsettled_runs().await })
    }

    fn list_revisions(&self, workflow_id: &str) -> Result<Vec<WorkflowRevision>, WorkflowError> {
        let (inner, id) = (Arc::clone(&self.inner), workflow_id.to_string());
        self.block(async move { inner.list_revisions(&id).await })
    }

    fn revision(
        &self,
        workflow_id: &str,
        revision_id: &str,
    ) -> Result<Option<WorkflowRevision>, WorkflowError> {
        let (inner, workflow, revision) = (
            Arc::clone(&self.inner),
            workflow_id.to_string(),
            revision_id.to_string(),
        );
        self.block(async move { inner.revision(&workflow, &revision).await })
    }

    fn list_notes(&self, workflow_id: &str) -> Result<Vec<WorkflowNote>, WorkflowError> {
        let (inner, id) = (Arc::clone(&self.inner), workflow_id.to_string());
        self.block(async move { inner.list_notes(&id).await })
    }

    fn append_note(&self, note: &WorkflowNote) -> Result<(), WorkflowError> {
        let (inner, note) = (Arc::clone(&self.inner), note.clone());
        self.block(async move { inner.append_note(&note).await })
    }

    fn supersede_note(
        &self,
        workflow_id: &str,
        note_id: &str,
        by: &str,
    ) -> Result<bool, WorkflowError> {
        let (inner, workflow, note, by) = (
            Arc::clone(&self.inner),
            workflow_id.to_string(),
            note_id.to_string(),
            by.to_string(),
        );
        self.block(async move { inner.supersede_note(&workflow, &note, &by).await })
    }

    fn save_proposal(&self, proposal: &WorkflowProposal) -> Result<(), WorkflowError> {
        let (inner, proposal) = (Arc::clone(&self.inner), proposal.clone());
        self.block(async move { inner.save_proposal(&proposal).await })
    }

    fn save_proposal_if_fingerprint(
        &self,
        proposal: &WorkflowProposal,
        expected_fingerprint: &str,
    ) -> Result<bool, WorkflowError> {
        let (inner, proposal, expected) = (
            Arc::clone(&self.inner),
            proposal.clone(),
            expected_fingerprint.to_string(),
        );
        self.block(async move {
            inner
                .save_proposal_if_fingerprint(&proposal, &expected)
                .await
        })
    }

    fn get_proposal(&self, id: &str) -> Result<Option<WorkflowProposal>, WorkflowError> {
        let (inner, id) = (Arc::clone(&self.inner), id.to_string());
        self.block(async move { inner.get_proposal(&id).await })
    }

    fn list_proposals(&self, workflow_id: &str) -> Result<Vec<WorkflowProposal>, WorkflowError> {
        let (inner, id) = (Arc::clone(&self.inner), workflow_id.to_string());
        self.block(async move { inner.list_proposals(&id).await })
    }

    fn lock_proposal_decision(
        &self,
        workflow_id: &str,
    ) -> Result<Box<dyn ProposalDecisionGuard>, WorkflowError> {
        let (inner, id) = (Arc::clone(&self.inner), workflow_id.to_string());
        let token = self.block(async move { inner.acquire_decision_lease(&id).await })?;
        Ok(Box::new(locks::DecisionLease::new(
            self.clone(),
            workflow_id.to_string(),
            token,
        )))
    }
}

/// How many notes a journal keeps; re-exported so a host can size around it.
pub const JOURNAL_CAP: usize = MAX_NOTES;

#[cfg(test)]
pub(crate) mod test_support;

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "history_tests.rs"]
mod history_tests;

#[cfg(test)]
#[path = "notes_tests.rs"]
mod notes_tests;

#[cfg(test)]
#[path = "runs_tests.rs"]
mod runs_tests;
