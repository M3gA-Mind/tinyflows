//! The [`WorkflowStore`] trait for [`DriverWorkflowStore`]: each call runs the
//! matching async half on the blocking bridge.

use std::sync::Arc;

use tinyflows::store::types::{
    RunRecord, WorkflowError, WorkflowNote, WorkflowProposal, WorkflowRecord, WorkflowRevision,
    WorkflowSummary,
};
use tinyflows::store::{ProposalDecisionGuard, WorkflowStore};

use super::{DriverWorkflowStore, locks};

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
        let held = self.block(async move { inner.acquire_decision_lease(&id).await })?;
        Ok(Box::new(locks::DecisionLease::new(
            self.clone(),
            workflow_id.to_string(),
            held,
        )))
    }
}
