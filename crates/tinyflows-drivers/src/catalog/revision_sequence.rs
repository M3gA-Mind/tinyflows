//! Store-owned sequence for the latest committed workflow graph revision.

use anyhow::Result;

use super::{DEFINITIONS, FlowCatalogDocuments, MAX_REVISIONS_PER_FLOW};

impl FlowCatalogDocuments {
    /// Counts visible prior snapshots for `flow_id`, bounded by the per-flow
    /// retention cap. This is used to bootstrap a legacy sequence, not as the
    /// latest revision number after a sequence has been persisted.
    pub async fn retained_history_count(&self, flow_id: &str) -> Result<u64> {
        Ok(self
            .list_revisions(flow_id, MAX_REVISIONS_PER_FLOW)
            .await?
            .len() as u64)
    }

    /// Returns the latest committed graph revision. A legacy definition with
    /// no stored sequence bootstraps at one plus its visible retained history;
    /// any older pruned history cannot be reconstructed. The next successful
    /// update persists that baseline and future updates remain monotonic.
    pub async fn latest_revision_number(&self, flow_id: &str) -> Result<Option<u64>> {
        let docs = self.docs().await?;
        let Some(stored) = docs.get(DEFINITIONS, flow_id).await? else {
            return Ok(None);
        };
        let revision = match stored
            .doc
            .get("revision")
            .and_then(serde_json::Value::as_u64)
        {
            Some(revision) => revision.max(1),
            None => self
                .retained_history_count(flow_id)
                .await?
                .saturating_add(1),
        };
        Ok(Some(revision))
    }
}

#[cfg(test)]
#[path = "revision_sequence_tests.rs"]
mod tests;
