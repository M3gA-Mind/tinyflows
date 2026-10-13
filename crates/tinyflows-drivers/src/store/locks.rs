//! The lease behind [`WorkflowStore::lock_proposal_decision`].
//!
//! A document per workflow, `{ owner, expires_at }`. Claimed by writing it where
//! none exists, or by swapping over one whose lease has run out; released by
//! deleting it while it is still ours. That is exclusion across every process
//! and store instance sharing the database, which the file store got from an
//! advisory file lock held across the decision.
//!
//! [`WorkflowStore::lock_proposal_decision`]: tinyflows::store::WorkflowStore::lock_proposal_decision

use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tinyflows::store::ProposalDecisionGuard;
use tinyflows::store::types::WorkflowError;
use tinystoragedrivers_core::Precondition;

use super::definitions::is_race;
use super::{DriverWorkflowStore, Inner, LOCKS, doc_id, now_ms, stamp, storage};

/// How long a claimed lease is good for. A decision that outlives it loses its
/// exclusion, which is what lets a crashed holder's lease be taken over.
pub const LOCK_LEASE: Duration = Duration::from_secs(120);

/// How long to wait for another holder before giving up.
const WAIT: Duration = Duration::from_secs(150);

/// Pause between attempts on a lease someone else holds.
const POLL: Duration = Duration::from_millis(15);

impl Inner {
    /// Claims the decision lease of `workflow_id`, waiting while another holder
    /// has it. Returns the token that proves ownership.
    pub(super) async fn acquire_decision_lease(
        &self,
        workflow_id: &str,
    ) -> Result<String, WorkflowError> {
        self.ensure().await?;
        let key = doc_id(workflow_id)?;
        let token = uuid::Uuid::new_v4().to_string();
        let started = Instant::now();
        loop {
            let now = now_ms();
            let lease = json!({
                "workflow_id": workflow_id,
                "owner": token,
                "expires_at": stamp(now.saturating_add(u64::try_from(LOCK_LEASE.as_millis()).unwrap_or(u64::MAX))),
            });
            let held = self.docs.get(LOCKS, &key).await.map_err(storage)?;
            let precondition = match &held {
                None => Some(Precondition::Absent),
                Some(stored) if expired(&stored.doc, now) => Some(stored.unchanged()),
                Some(_) => None,
            };
            if let Some(precondition) = precondition {
                match self.docs.put(LOCKS, &key, lease, precondition).await {
                    Ok(_) => return Ok(token),
                    Err(error) if is_race(error.kind()) => {}
                    Err(error) => return Err(storage(error)),
                }
            }
            if started.elapsed() >= WAIT {
                return Err(WorkflowError::Storage(format!(
                    "timed out waiting for the decision lease on '{workflow_id}'"
                )));
            }
            tokio::time::sleep(POLL).await;
        }
    }

    /// Releases `token`'s lease on `workflow_id`, if it is still ours.
    pub(super) async fn release_decision_lease(&self, workflow_id: &str, token: &str) {
        let Ok(key) = doc_id(workflow_id) else { return };
        let released = async {
            let Some(stored) = self.docs.get(LOCKS, &key).await? else {
                return Ok(());
            };
            if stored.doc.get("owner").and_then(Value::as_str) == Some(token) {
                self.docs.delete(LOCKS, &key, stored.unchanged()).await?;
            }
            Ok::<(), tinystoragedrivers_core::StorageError>(())
        }
        .await;
        if let Err(error) = released {
            tracing::warn!(target: "workflows", workflow_id, %error, "[workflows] failed to release a decision lease; it expires on its own");
        }
    }
}

fn expired(lease: &Value, now: u64) -> bool {
    lease
        .get("expires_at")
        .and_then(Value::as_i64)
        .and_then(|at| u64::try_from(at).ok())
        .is_none_or(|at| at <= now)
}

/// Holding the lease is the operation; dropping releases it.
pub(super) struct DecisionLease {
    store: DriverWorkflowStore,
    workflow_id: String,
    token: String,
}

impl DecisionLease {
    pub(super) fn new(store: DriverWorkflowStore, workflow_id: String, token: String) -> Self {
        Self {
            store,
            workflow_id,
            token,
        }
    }
}

impl ProposalDecisionGuard for DecisionLease {}

impl Drop for DecisionLease {
    fn drop(&mut self) {
        let inner = std::sync::Arc::clone(&self.store.inner);
        let (workflow_id, token) = (self.workflow_id.clone(), self.token.clone());
        let released = self.store.block(async move {
            inner.release_decision_lease(&workflow_id, &token).await;
            Ok(())
        });
        if let Err(error) = released {
            tracing::warn!(target: "workflows", workflow_id = %self.workflow_id, %error, "[workflows] could not run a decision lease release; it expires on its own");
        }
    }
}
