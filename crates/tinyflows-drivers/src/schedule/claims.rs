//! Atomic occurrence leases and fenced run completion.
mod completion;

use super::codec::{INCARNATION, doc_to_job, incarnation, run_id, run_to_doc};
use super::{CAS_ATTEMPTS, CronDocuments, JOBS, RUNS, storage_error};
use anyhow::{Result, bail};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tinyflows_schedule::CronJob;
use tinystoragedrivers_core::{Capability, Precondition, Versioned, WriteOp};
use uuid::Uuid;

const LEASE: &str = "occurrence_lease";
const COMPLETED: &str = "completed_occurrence";
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Lease {
    owner: String,
    token: String,
    incarnation: String,
    occurrence: DateTime<Utc>,
    started_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    #[serde(default)]
    queued_run: Option<i64>,
}

/// A fenced occurrence captured before dispatch. Keep this value through
/// execution and pass it to completion; reading a fresh job loses that fence.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimedOccurrence {
    job: CronJob,
    lease: Lease,
}
impl ClaimedOccurrence {
    /// The exact job snapshot acquired for dispatch.
    pub fn job(&self) -> &CronJob {
        &self.job
    }
    /// The worker that acquired this occurrence.
    pub fn owner(&self) -> &str {
        &self.lease.owner
    }
    /// The random fencing token, changed on each acquisition or recovery.
    pub fn token(&self) -> &str {
        &self.lease.token
    }
    /// The acquired job incarnation.
    pub fn incarnation(&self) -> &str {
        &self.lease.incarnation
    }
    /// The exact scheduled instant, without millisecond rounding.
    pub fn occurrence(&self) -> DateTime<Utc> {
        self.lease.occurrence
    }
    /// Start instant captured with this lease.
    pub fn started_at(&self) -> DateTime<Utc> {
        self.lease.started_at
    }
    /// The initial expiry. Renewal extends the stored lease; this snapshot
    /// remains the original acquisition receipt.
    pub fn expires_at(&self) -> DateTime<Utc> {
        self.lease.expires_at
    }
}
fn expiry(now: DateTime<Utc>, duration: Duration) -> Result<DateTime<Utc>> {
    if duration <= Duration::zero() {
        bail!("cron store: lease duration must be positive");
    }
    now.checked_add_signed(duration)
        .ok_or_else(|| anyhow::anyhow!("cron store: lease expiry overflow"))
}
fn lease(doc: &Value) -> Result<Option<Lease>> {
    doc.get(LEASE)
        .cloned()
        .map(serde_json::from_value)
        .transpose()
        .map_err(Into::into)
}

pub(super) fn queued_run_cleanup(doc: &Value) -> Result<Vec<WriteOp>> {
    let Some(queued_run) = lease(doc)?.and_then(|lease| lease.queued_run) else {
        return Ok(vec![]);
    };
    Ok(vec![WriteOp::Delete {
        collection: RUNS.into(),
        id: run_id(queued_run),
        precondition: Precondition::None,
    }])
}

fn owns(doc: &Value, claim: &ClaimedOccurrence, now: DateTime<Utc>) -> Result<bool> {
    let Some(current) = lease(doc)? else {
        return Ok(false);
    };
    Ok(current.token == claim.lease.token
        && current.owner == claim.lease.owner
        && current.incarnation == claim.lease.incarnation
        && incarnation(doc) == Some(claim.lease.incarnation.as_str())
        && current.occurrence == claim.lease.occurrence
        && current.expires_at > now
        && doc_to_job(&Versioned {
            id: claim.job.id.clone(),
            version: tinystoragedrivers_core::Version(0),
            doc: doc.clone(),
        })?
        .next_run
            == claim.lease.occurrence)
}
impl CronDocuments {
    /// Acquires one enabled, due occurrence, or recovers its expired lease.
    /// Only one worker succeeds across handles sharing this scope. The host
    /// supplies now; clocks must agree within its lease tolerance.
    /// Recovery is at least once: a crash after an external send but before
    /// completion can repeat that send. This is not an HTTP exactly-once API.
    ///
    /// # Errors
    /// Invalid owner/duration, malformed state, unavailable transactions,
    /// exhausted contention retries, or backend failure.
    pub async fn claim_due_job(
        &self,
        job_id: &str,
        owner: &str,
        now: DateTime<Utc>,
        duration: Duration,
    ) -> Result<Option<ClaimedOccurrence>> {
        self.claim_occurrence(job_id, None, owner, now, duration, false)
            .await
    }

    /// Due-only claim that also verifies the caller's captured definition.
    pub async fn claim_due_job_for(
        &self,
        expected: &CronJob,
        owner: &str,
        now: DateTime<Utc>,
        duration: Duration,
    ) -> Result<Option<ClaimedOccurrence>> {
        self.claim_occurrence(&expected.id, Some(expected), owner, now, duration, false)
            .await
    }

    /// Atomically claims a saved definition for an explicit manual run,
    /// including disabled jobs and jobs scheduled in the future.
    pub async fn claim_job_now(
        &self,
        expected: &CronJob,
        owner: &str,
        now: DateTime<Utc>,
        duration: Duration,
    ) -> Result<Option<ClaimedOccurrence>> {
        self.claim_occurrence(&expected.id, Some(expected), owner, now, duration, true)
            .await
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
            if let Some(lease) = lease(&stored.doc)?
                && lease.owner == owner
                && lease.expires_at > now
            {
                return Ok(Some(ClaimedOccurrence { job, lease }));
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
                Err(error) if super::admission::conflict(&error) => {}
                Err(error) => return Err(error),
            }
        }
        bail!("cron store: manual admission reconciliation kept changing")
    }

    async fn claim_occurrence(
        &self,
        job_id: &str,
        expected: Option<&CronJob>,
        owner: &str,
        now: DateTime<Utc>,
        duration: Duration,
        force: bool,
    ) -> Result<Option<ClaimedOccurrence>> {
        if owner.trim().is_empty() {
            bail!("cron store: lease owner must not be empty");
        }
        let expires_at = expiry(now, duration)?;
        self.docs
            .capabilities()
            .require(Capability::Transactions)
            .map_err(storage_error)?;
        self.ensure().await?;
        for _ in 0..CAS_ATTEMPTS {
            let Some(stored) = self.docs.get(JOBS, job_id).await.map_err(storage_error)? else {
                return Ok(None);
            };
            let job = doc_to_job(&stored)?;
            if expected.is_some_and(|expected| {
                job.created_at != expected.created_at
                    || job.claim_generation != expected.claim_generation
                    || !job.same_execution_definition(expected)
                    || job.schedule != expected.schedule
                    || job.next_run != expected.next_run
            }) {
                return Ok(None);
            }
            if !force && (!job.enabled || job.next_run > now) {
                return Ok(None);
            }
            let previous_lease = lease(&stored.doc)?;
            if previous_lease
                .as_ref()
                .is_some_and(|lease| lease.expires_at > now)
            {
                return Ok(None);
            }
            let mut doc = stored.doc.clone();
            let incarnation = incarnation(&doc)
                .map(str::to_owned)
                .unwrap_or_else(|| Uuid::new_v4().to_string());
            doc[INCARNATION] = Value::String(incarnation.clone());
            doc.as_object_mut()
                .ok_or_else(|| anyhow::anyhow!("cron store: invalid job"))?
                .remove(COMPLETED);
            let acquired = Lease {
                owner: owner.into(),
                token: Uuid::new_v4().to_string(),
                incarnation,
                occurrence: job.next_run,
                started_at: now,
                expires_at,
                queued_run: None,
            };
            doc[LEASE] = serde_json::to_value(&acquired)?;
            let mut extra = vec![];
            if let Some(queued_run) = previous_lease.and_then(|lease| lease.queued_run) {
                extra.push(WriteOp::Delete {
                    collection: RUNS.into(),
                    id: run_id(queued_run),
                    precondition: Precondition::None,
                });
            }
            match self
                .commit_job(job_id, Some(&stored), Some(doc), extra)
                .await
            {
                Ok(_) => {
                    return Ok(Some(ClaimedOccurrence {
                        job,
                        lease: acquired,
                    }));
                }
                Err(error) if super::admission::conflict(&error) => {}
                Err(error) => return Err(error),
            }
        }
        bail!("cron store: occurrence kept changing during acquisition")
    }

    /// Appends a durable queued placeholder while preserving the lease. The
    /// scheduler replaces this row with terminal history when it completes.
    pub async fn queue_claimed_run(
        &self,
        claim: &ClaimedOccurrence,
        queued_at: DateTime<Utc>,
    ) -> Result<bool> {
        self.docs
            .capabilities()
            .require(Capability::Transactions)
            .map_err(storage_error)?;
        self.ensure().await?;
        for _ in 0..CAS_ATTEMPTS {
            let Some(stored) = self
                .docs
                .get(JOBS, &claim.job.id)
                .await
                .map_err(storage_error)?
            else {
                return Ok(false);
            };
            if !owns(&stored.doc, claim, queued_at)? {
                return Ok(false);
            }
            let mut doc = stored.doc.clone();
            if lease(&stored.doc)?.is_some_and(|lease| lease.queued_run.is_some()) {
                return Ok(true);
            }
            let seq = self.next_run_number().await?;
            doc[LEASE]["queued_run"] = serde_json::json!(seq);
            let run = run_to_doc(
                seq,
                &claim.job.id,
                Some(&claim.lease.incarnation),
                queued_at,
                queued_at,
                "queued",
                None,
                0,
                None,
            );
            let extra = vec![WriteOp::Put {
                collection: RUNS.into(),
                id: run_id(seq),
                doc: run,
                precondition: Precondition::Absent,
            }];
            match self
                .commit_job(&claim.job.id, Some(&stored), Some(doc), extra)
                .await
            {
                Ok(_) => return Ok(true),
                Err(error) if super::admission::conflict(&error) => {}
                Err(error) => return Err(error),
            }
        }
        bail!("cron store: occurrence changed while queueing run")
    }

    /// Extends an unexpired, still-owned occurrence. A stale worker receives
    /// false and must stop; it cannot renew a successor lease.
    ///
    /// # Errors
    /// Invalid duration, malformed state, exhausted retries, or backend failure.
    pub async fn renew_claim(
        &self,
        claim: &ClaimedOccurrence,
        now: DateTime<Utc>,
        duration: Duration,
    ) -> Result<bool> {
        let expires_at = expiry(now, duration)?;
        self.change_claim(claim, now, Some(expires_at)).await
    }
    /// Releases an unexpired claim without advancing it. The occurrence can
    /// then be acquired again with a new token.
    ///
    /// # Errors
    /// Malformed state, unavailable transactions, exhausted retries, or backend failure.
    pub async fn release_claim(
        &self,
        claim: &ClaimedOccurrence,
        now: DateTime<Utc>,
    ) -> Result<bool> {
        self.change_claim(claim, now, None).await
    }
    async fn change_claim(
        &self,
        claim: &ClaimedOccurrence,
        now: DateTime<Utc>,
        expires_at: Option<DateTime<Utc>>,
    ) -> Result<bool> {
        self.docs
            .capabilities()
            .require(Capability::Transactions)
            .map_err(storage_error)?;
        self.ensure().await?;
        for _ in 0..CAS_ATTEMPTS {
            let Some(stored) = self
                .docs
                .get(JOBS, &claim.job.id)
                .await
                .map_err(storage_error)?
            else {
                return Ok(false);
            };
            if !owns(&stored.doc, claim, now)? {
                return Ok(false);
            }
            let mut doc = stored.doc.clone();
            let mut extra = vec![];
            match expires_at {
                Some(expires_at) => {
                    let current_expiry = lease(&stored.doc)?
                        .ok_or_else(|| anyhow::anyhow!("cron store: missing lease"))?
                        .expires_at;
                    doc[LEASE]["expires_at"] =
                        serde_json::to_value(expires_at.max(current_expiry))?;
                }
                None => {
                    if let Some(queued_run) = lease(&stored.doc)?.and_then(|lease| lease.queued_run)
                    {
                        extra.push(WriteOp::Delete {
                            collection: RUNS.into(),
                            id: run_id(queued_run),
                            precondition: Precondition::None,
                        });
                    }
                    doc.as_object_mut()
                        .ok_or_else(|| anyhow::anyhow!("cron store: invalid job"))?
                        .remove(LEASE);
                }
            }
            match self
                .commit_job(&claim.job.id, Some(&stored), Some(doc), extra)
                .await
            {
                Ok(_) => return Ok(true),
                Err(error) if super::admission::conflict(&error) => {}
                Err(error) => return Err(error),
            }
        }
        bail!("cron store: occurrence kept changing during lease update")
    }

    /// Deletes the job completed by this receipt only while its incarnation
    /// and completion token still match and no later acquisition or edit has
    /// invalidated it. Use this for `delete_after_run` after successful
    /// completion. A replaced or changed job returns `false`.
    ///
    /// Deletion and admission are atomic. History cleanup is a subsequent
    /// write restricted to the removed incarnation, preserving replacements.
    ///
    /// # Errors
    /// Malformed state, unavailable transactions, exhausted retries, or backend
    /// failure. A history-cleanup failure can occur after the job was deleted.
    pub async fn remove_completed_claim(&self, claim: &ClaimedOccurrence) -> Result<bool> {
        self.docs
            .capabilities()
            .require(Capability::Transactions)
            .map_err(storage_error)?;
        self.ensure().await?;
        for _ in 0..CAS_ATTEMPTS {
            let Some(stored) = self
                .docs
                .get(JOBS, &claim.job.id)
                .await
                .map_err(storage_error)?
            else {
                return Ok(false);
            };
            let completed: Option<Lease> = stored
                .doc
                .get(COMPLETED)
                .cloned()
                .map(serde_json::from_value)
                .transpose()?;
            let Some(completed) = completed else {
                return Ok(false);
            };
            if completed.token != claim.lease.token
                || completed.owner != claim.lease.owner
                || completed.incarnation != claim.lease.incarnation
                || completed.occurrence != claim.lease.occurrence
                || incarnation(&stored.doc) != Some(claim.lease.incarnation.as_str())
                || lease(&stored.doc)?.is_some()
            {
                return Ok(false);
            }
            if self.remove_unchanged(&stored).await? {
                return Ok(true);
            }
        }
        bail!("cron store: completed occurrence kept changing during removal")
    }
}
