//! Fence an ambiguous manual admission without changing a replacement job.
use super::*;

impl CronDocuments {
    /// Verify that manual occurrence admission is available before a host
    /// starts an operation whose callback response may be ambiguous.
    pub fn require_manual_claim_support(&self) -> Result<()> {
        self.docs
            .capabilities()
            .require(Capability::Transactions)
            .map_err(storage_error)
    }

    /// Reconcile an ambiguous manual-claim response. It returns a live lease
    /// for `owner`; otherwise a same-document CAS increments the claim
    /// generation, fencing any delayed claim transaction with the captured
    /// job snapshot before returning `None`.
    pub async fn reconcile_manual_claim_admission(
        &self,
        expected: &CronJob,
        owner: &str,
        now: DateTime<Utc>,
    ) -> Result<Option<ClaimedOccurrence>> {
        self.require_manual_claim_support()?;
        if owner.trim().is_empty() {
            bail!("cron store: lease owner must not be empty");
        }
        for _ in 0..CAS_ATTEMPTS {
            let Some(stored) = self
                .docs
                .get(JOBS, &expected.id)
                .await
                .map_err(storage_error)?
            else {
                return Ok(None);
            };
            let job = doc_to_job(&stored)?;
            if let Some(current_lease) = lease(&stored.doc)? {
                if current_lease.owner == owner && current_lease.expires_at > now {
                    return Ok(Some(ClaimedOccurrence {
                        job,
                        lease: current_lease,
                    }));
                }
                if current_lease.expires_at > now {
                    return Ok(None);
                }
            }
            if job.created_at != expected.created_at
                || job.claim_generation != expected.claim_generation
                || !job.same_execution_definition(expected)
                || job.schedule != expected.schedule
                || job.next_run != expected.next_run
            {
                return Ok(None);
            }

            let generation = job
                .claim_generation
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("cron store: claim generation exhausted"))?;
            let mut doc = stored.doc.clone();
            doc["claim_generation"] = generation.into();
            match self
                .commit_job(&job.id, Some(&stored), Some(doc), vec![])
                .await
            {
                Ok(_) => return Ok(None),
                Err(error) if super::super::admission::conflict(&error) => {}
                Err(error) => return Err(error),
            }
        }
        bail!("cron store: manual admission reconciliation kept changing")
    }
}
