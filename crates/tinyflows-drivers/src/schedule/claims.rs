//! Atomic occurrence leases and fenced run completion.
//!
//! A claim is a `tsd.leases` record (owner `cron`, key
//! `Key::of([job_id, occurrence_ms])`) held by a random token, so one occurrence
//! has one live holder across handles sharing the scope. The lease's epoch
//! moves on every new tenure, which is what fences a stalled worker. The job
//! document carries only the receipt of the last completed claim
//! (`completed_occurrence`), which `remove_completed_claim` checks.
use super::codec::{
    INCARNATION, doc_to_job, from_ms, incarnation, integer, run_id, run_to_doc, set_last_run,
};
use super::jobs::reschedule;
use super::{CAS_ATTEMPTS, CronDocuments, JOBS, OWNER, RUNS, storage_error};
use anyhow::{Result, bail};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tinyflows_schedule::{
    CronJob, DeliveryStatus, Schedule, next_run_for_schedule, truncate_cron_output,
};
use tinystoragedrivers_core::{
    Capability, Key, Lease, Leases, Precondition, Versioned, WriteOp, primitives,
};
use uuid::Uuid;

const COMPLETED: &str = "completed_occurrence";

/// The receipt of a completed claim, kept on the job document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
    owner: String,
    token: String,
    epoch: u64,
    incarnation: String,
    occurrence_ms: i64,
}

/// What a claim captured when it was acquired.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ClaimLease {
    owner: String,
    token: String,
    epoch: u64,
    incarnation: String,
    occurrence: DateTime<Utc>,
    started_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
}

/// A fenced occurrence captured before dispatch. Keep this value through
/// execution and pass it to completion; reading a fresh job loses that fence.
#[derive(Debug, Clone)]
pub struct ClaimedOccurrence {
    job: CronJob,
    lease: ClaimLease,
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
    /// The random fencing token (the lease holder), changed on each
    /// acquisition or recovery.
    pub fn token(&self) -> &str {
        &self.lease.token
    }
    /// The acquired job incarnation.
    pub fn incarnation(&self) -> &str {
        &self.lease.incarnation
    }
    /// The scheduled instant (millisecond precision).
    pub fn occurrence(&self) -> DateTime<Utc> {
        self.lease.occurrence
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

/// The `tsd.leases` key of `job_id`'s occurrence at `occurrence_ms`.
fn lease_key(job_id: &str, occurrence_ms: i64) -> Result<String> {
    Ok(Key::of([job_id, occurrence_ms.to_string().as_str()])?
        .as_str()
        .to_owned())
}

fn receipt(doc: &Value) -> Result<Option<Receipt>> {
    doc.get(COMPLETED)
        .cloned()
        .map(serde_json::from_value)
        .transpose()
        .map_err(Into::into)
}

impl CronDocuments {
    /// Leases whose clock is the host-supplied `now`, as the claim API always
    /// took the time from its caller.
    fn leases_at(&self, now: DateTime<Utc>) -> Leases {
        let ms = u64::try_from(now.timestamp_millis()).unwrap_or_default();
        Leases::new(&self.docs).with_clock(std::sync::Arc::new(move || ms))
    }

    /// Whether `claim` still holds the live lease of its occurrence on the
    /// current, same-incarnation job whose next run is that occurrence.
    async fn owns(
        &self,
        stored: &Versioned<Value>,
        claim: &ClaimedOccurrence,
        now: DateTime<Utc>,
    ) -> Result<bool> {
        if incarnation(&stored.doc) != Some(claim.lease.incarnation.as_str())
            || doc_to_job(stored)?.next_run != claim.lease.occurrence
        {
            return Ok(false);
        }
        let key = lease_key(&claim.job.id, claim.lease.occurrence.timestamp_millis())?;
        let held = self
            .leases_at(now)
            .current(OWNER, &key)
            .await
            .map_err(storage_error)?;
        Ok(held.is_some_and(|held| {
            held.holder == claim.lease.token && held.epoch == claim.lease.epoch
        }))
    }

    /// Ends the live lease of the occurrence `job_doc` is due at, whoever
    /// holds it: an edit, removal or re-creation of the job invalidates every
    /// claim taken on its earlier shape. Best effort and clock-independent.
    pub(super) async fn fence_claims(&self, job_id: &str, job_doc: &Value) {
        let Some(occurrence) = job_doc.get("next_run_ms").and_then(Value::as_i64) else {
            return;
        };
        let Ok(key) = lease_key(job_id, occurrence) else {
            return;
        };
        let Ok(id) = Key::of([OWNER, key.as_str()]) else {
            return;
        };
        let Ok(Some(found)) = self.docs.get(primitives::LEASES, id.as_str()).await else {
            return;
        };
        let mut body = found.doc.clone();
        if body.get("expires_ms").and_then(Value::as_i64) == Some(0) {
            return;
        }
        body["expires_ms"] = json!(0);
        // A conflict means the lease moved on; it is no longer the one fenced.
        let _ = self
            .docs
            .put(primitives::LEASES, id.as_str(), body, found.unchanged())
            .await;
    }

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
        if owner.trim().is_empty() {
            bail!("cron store: lease owner must not be empty");
        }
        let expires_at = expiry(now, duration)?;
        self.docs
            .capabilities()
            .require(Capability::Transactions)
            .map_err(storage_error)?;
        self.ensure().await?;
        let leases = self.leases_at(now);
        let ttl_ms = duration.num_milliseconds().max(1);
        for _ in 0..CAS_ATTEMPTS {
            let Some(stored) = self.docs.get(JOBS, job_id).await.map_err(storage_error)? else {
                return Ok(None);
            };
            let job = doc_to_job(&stored)?;
            if !job.enabled || job.next_run > now {
                return Ok(None);
            }
            let key = lease_key(job_id, job.next_run.timestamp_millis())?;
            let token = Uuid::new_v4().to_string();
            let Some(lease) = leases
                .acquire(OWNER, &key, &token, ttl_ms)
                .await
                .map_err(storage_error)?
            else {
                return Ok(None);
            };
            let incarnation = incarnation(&stored.doc)
                .map(str::to_owned)
                .unwrap_or_else(|| Uuid::new_v4().to_string());
            // The job changes only when it has to: to mint its incarnation or
            // to drop the receipt of the previous completion. Otherwise the
            // lease must still be of the job as read.
            let needs_write = super::codec::incarnation(&stored.doc).is_none()
                || stored.doc.get(COMPLETED).is_some();
            let committed = if needs_write {
                let mut doc = stored.doc.clone();
                doc[INCARNATION] = Value::String(incarnation.clone());
                doc.as_object_mut()
                    .ok_or_else(|| anyhow::anyhow!("cron store: invalid job"))?
                    .remove(COMPLETED);
                match self
                    .commit_job(job_id, Some(&stored), Some(doc), vec![])
                    .await
                {
                    Ok(_) => true,
                    Err(error) if super::admission::conflict(&error) => false,
                    Err(error) => {
                        self.give_back(&leases, &key, &lease).await;
                        return Err(error);
                    }
                }
            } else {
                self.docs
                    .get(JOBS, job_id)
                    .await
                    .map_err(storage_error)?
                    .is_some_and(|fresh| fresh.doc == stored.doc)
            };
            if !committed {
                self.give_back(&leases, &key, &lease).await;
                continue;
            }
            return Ok(Some(ClaimedOccurrence {
                job,
                lease: ClaimLease {
                    owner: owner.into(),
                    token,
                    epoch: lease.epoch,
                    incarnation,
                    occurrence: from_ms(job_occurrence_ms(&stored.doc)?)?,
                    started_at: now,
                    expires_at,
                },
            }));
        }
        bail!("cron store: occurrence kept changing during acquisition")
    }

    async fn give_back(&self, leases: &Leases, key: &str, lease: &Lease) {
        let _ = leases.release(OWNER, key, &lease.holder, lease.epoch).await;
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
        let Some(stored) = self
            .docs
            .get(JOBS, &claim.job.id)
            .await
            .map_err(storage_error)?
        else {
            return Ok(false);
        };
        if !self.owns(&stored, claim, now).await? {
            return Ok(false);
        }
        let key = lease_key(&claim.job.id, claim.lease.occurrence.timestamp_millis())?;
        let leases = self.leases_at(now);
        match expires_at {
            Some(expires_at) => {
                // Renewal never shortens an existing lease.
                let current = leases
                    .current(OWNER, &key)
                    .await
                    .map_err(storage_error)?
                    .map_or(0, |held| held.expires_ms);
                let wanted = expires_at.timestamp_millis().max(current);
                let ttl_ms = (wanted - now.timestamp_millis()).max(1);
                Ok(leases
                    .renew(OWNER, &key, &claim.lease.token, claim.lease.epoch, ttl_ms)
                    .await
                    .map_err(storage_error)?
                    .is_some())
            }
            None => leases
                .release(OWNER, &key, &claim.lease.token, claim.lease.epoch)
                .await
                .map_err(storage_error),
        }
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
            let Some(completed) = receipt(&stored.doc)? else {
                return Ok(false);
            };
            if completed.token != claim.lease.token
                || completed.owner != claim.lease.owner
                || completed.incarnation != claim.lease.incarnation
                || completed.epoch != claim.lease.epoch
                || completed.occurrence_ms != claim.lease.occurrence.timestamp_millis()
                || incarnation(&stored.doc) != Some(claim.lease.incarnation.as_str())
            {
                return Ok(false);
            }
            if self.remove_unchanged(&stored).await? {
                return Ok(true);
            }
        }
        bail!("cron store: completed occurrence kept changing during removal")
    }

    /// Atomically records a run and advances its still-owned occurrence.
    /// Stale, expired, edited, canceled or re-created jobs return false
    /// without any outcome write. A one-shot is disabled, retaining history.
    /// The host applies delete_after_run with [`Self::remove_completed_claim`]
    /// separately when it wants removal. Never follow completion with
    /// `remove_job(job_id)`: that id may already belong to a replacement.
    ///
    /// # Errors
    /// Completion predates acquisition, state/schedule is malformed, transactions
    /// are unavailable, retries are exhausted, or the backend fails. Pruning
    /// failure is reported after the run and advance have already committed.
    pub async fn complete_claim(
        &self,
        claim: &ClaimedOccurrence,
        finished_at: DateTime<Utc>,
        success: bool,
        output: &str,
        delivery_status: Option<DeliveryStatus>,
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
            if !self.owns(&stored, claim, finished_at).await? {
                return Ok(false);
            }
            let job = doc_to_job(&stored)?;
            let mut doc = stored.doc.clone();
            let body = doc
                .as_object_mut()
                .ok_or_else(|| anyhow::anyhow!("cron store: invalid job"))?;
            body.insert(
                COMPLETED.into(),
                serde_json::to_value(Receipt {
                    owner: claim.lease.owner.clone(),
                    token: claim.lease.token.clone(),
                    epoch: claim.lease.epoch,
                    incarnation: claim.lease.incarnation.clone(),
                    occurrence_ms: claim.lease.occurrence.timestamp_millis(),
                })?,
            );
            set_last_run(body, finished_at, success, truncate_cron_output(output));
            reschedule(
                body,
                next_run_for_schedule(&job.schedule, finished_at)?,
                matches!(job.schedule, Schedule::At { .. }),
            );
            let seq = self.next_run_number().await?;
            let run = run_to_doc(
                seq,
                &job.id,
                Some(&claim.lease.incarnation),
                claim.lease.started_at,
                finished_at,
                if success { "success" } else { "error" },
                Some(truncate_cron_output(output)),
                (finished_at - claim.lease.started_at).num_milliseconds(),
                delivery_status.as_ref(),
            );
            let extra = vec![WriteOp::Put {
                collection: RUNS.into(),
                id: run_id(seq),
                doc: run,
                precondition: Precondition::Absent,
            }];
            match self
                .commit_job(&job.id, Some(&stored), Some(doc), extra)
                .await
            {
                Ok(_) => {
                    // The occurrence is done: end its lease so the receipt is
                    // the only trace of this claim.
                    let key = lease_key(&job.id, claim.lease.occurrence.timestamp_millis())?;
                    let _ = self
                        .leases_at(finished_at)
                        .release(OWNER, &key, &claim.lease.token, claim.lease.epoch)
                        .await;
                    self.prune_runs(&job.id, &stored.doc).await?;
                    return Ok(true);
                }
                Err(error) if super::admission::conflict(&error) => {}
                Err(error) => return Err(error),
            }
        }
        bail!("cron store: occurrence kept changing during completion")
    }
}

/// The scheduled occurrence stored on a job document, in epoch milliseconds.
fn job_occurrence_ms(doc: &Value) -> Result<i64> {
    integer(doc, "next_run_ms")?
        .ok_or_else(|| anyhow::anyhow!("cron store: document has no `next_run_ms`"))
}
