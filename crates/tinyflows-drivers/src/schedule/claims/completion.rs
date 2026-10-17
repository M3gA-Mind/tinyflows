//! Fenced terminal history and schedule mutation for occurrence leases.
use super::{ClaimedOccurrence, lease, owns};
use super::super::codec::{doc_to_job, run_id, run_to_doc, set_last_run};
use super::super::jobs::reschedule;
use super::super::{CAS_ATTEMPTS, CronDocuments, JOBS, RUNS, storage_error};
use anyhow::{Result, bail};
use chrono::DateTime;
use tinyflows_schedule::{DeliveryStatus, Schedule, next_run_for_schedule, truncate_cron_output};
use tinystoragedrivers_core::{Capability, Precondition, WriteOp};

impl CronDocuments {
    /// Atomically records a run and advances its still-owned occurrence.
    /// Stale, expired, edited, canceled or re-created jobs return false
    /// without any outcome write. A one-shot is disabled, retaining history.
    /// The host applies delete-after-run with [`Self::remove_completed_claim`]
    /// separately when it wants removal.
    pub async fn complete_claim(
        &self,
        claim: &ClaimedOccurrence,
        finished_at: DateTime<chrono::Utc>,
        success: bool,
        output: &str,
        delivery_status: Option<DeliveryStatus>,
    ) -> Result<bool> {
        self.complete_claim_mode(claim, finished_at, success, output, delivery_status, true)
            .await
    }

    /// Atomically record a manual run without advancing or mutating its saved
    /// schedule, enabled state, or delete-after-run policy.
    pub async fn complete_manual_claim(
        &self,
        claim: &ClaimedOccurrence,
        finished_at: DateTime<chrono::Utc>,
        success: bool,
        output: &str,
        delivery_status: Option<DeliveryStatus>,
    ) -> Result<bool> {
        self.complete_claim_mode(claim, finished_at, success, output, delivery_status, false)
            .await
    }

    async fn complete_claim_mode(
        &self,
        claim: &ClaimedOccurrence,
        finished_at: DateTime<chrono::Utc>,
        success: bool,
        output: &str,
        delivery_status: Option<DeliveryStatus>,
        advance_schedule: bool,
    ) -> Result<bool> {
        self.docs
            .capabilities()
            .require(Capability::Transactions)
            .map_err(storage_error)?;
        self.ensure().await?;
        if finished_at < claim.lease.started_at {
            bail!("cron store: completion precedes acquisition");
        }
        for _ in 0..CAS_ATTEMPTS {
            let Some(stored) = self
                .docs
                .get(JOBS, &claim.job.id)
                .await
                .map_err(storage_error)?
            else {
                return Ok(false);
            };
            if !owns(&stored.doc, claim, finished_at)? {
                return Ok(false);
            }
            let job = doc_to_job(&stored)?;
            let mut doc = stored.doc.clone();
            let body = doc
                .as_object_mut()
                .ok_or_else(|| anyhow::anyhow!("cron store: invalid job"))?;
            body.remove(super::LEASE);
            body.insert(
                super::COMPLETED.into(),
                serde_json::to_value(&claim.lease)?,
            );
            set_last_run(body, finished_at, success, truncate_cron_output(output));
            if advance_schedule {
                reschedule(
                    body,
                    next_run_for_schedule(&job.schedule, finished_at)?,
                    matches!(job.schedule, Schedule::At { .. }),
                );
            }
            let seq = self.next_run_number().await?;
            let run = run_to_doc(
                seq,
                &job.id,
                Some(&claim.lease.incarnation),
                claim.lease.started_at,
                finished_at,
                if success { "ok" } else { "error" },
                Some(truncate_cron_output(output)),
                (finished_at - claim.lease.started_at).num_milliseconds(),
                delivery_status.as_ref(),
            );
            let mut extra = vec![WriteOp::Put {
                collection: RUNS.into(),
                id: run_id(seq),
                doc: run,
                precondition: Precondition::Absent,
            }];
            if let Some(queued_run) = lease(&stored.doc)?.and_then(|lease| lease.queued_run) {
                extra.push(WriteOp::Delete {
                    collection: RUNS.into(),
                    id: run_id(queued_run),
                    precondition: Precondition::None,
                });
            }
            match self
                .commit_job(&job.id, Some(&stored), Some(doc), extra)
                .await
            {
                Ok(_) => {
                    self.prune_runs(&job.id, &stored.doc).await?;
                    return Ok(true);
                }
                Err(error) if super::super::admission::conflict(&error) => {}
                Err(error) => return Err(error),
            }
        }
        bail!("cron store: occurrence kept changing during completion")
    }
}
