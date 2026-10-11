//! Shared admission state, transacted with job writes to fence policy races.
use super::codec::{doc_to_job, incarnation};
use super::{CAS_ATTEMPTS, COUNTERS, CronDocuments, JOBS, storage_error};
use anyhow::{Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tinystoragedrivers_core::{
    Capability, DocumentStoreExt, ErrorKind, Precondition, Query, Versioned, WriteOp, WriteResult,
};

const ADMISSION: &str = "admission";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Member {
    incarnation: Option<String>,
    enabled: bool,
}
#[derive(Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Admission {
    limit: Option<usize>,
    jobs: std::collections::BTreeMap<String, Member>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AdmissionRecord {
    limit: Option<usize>,
    // Job ids can contain Mongo-sensitive key characters. Keep the map in a
    // JSON string, matching the cron store's existing nested-value convention.
    members_json: String,
}
fn decode(doc: Value) -> Result<Admission> {
    let record: AdmissionRecord = serde_json::from_value(doc)?;
    Ok(Admission {
        limit: record.limit,
        jobs: serde_json::from_str(&record.members_json)?,
    })
}
fn encode(state: Admission) -> Result<Value> {
    Ok(serde_json::to_value(AdmissionRecord {
        limit: state.limit,
        members_json: serde_json::to_string(&state.jobs)?,
    })?)
}
impl Admission {
    fn check(&self) -> Result<()> {
        if let Some(limit) = self.limit
            && (limit == 0 || self.jobs.values().filter(|member| member.enabled).count() > limit)
        {
            bail!("cron store: active job limit exceeded");
        }
        Ok(())
    }
}
fn member(doc: &Value) -> Result<Member> {
    Ok(Member {
        incarnation: incarnation(doc).map(str::to_owned),
        enabled: match doc.get("enabled") {
            None => false,
            Some(Value::Bool(enabled)) => *enabled,
            Some(_) => return Err(anyhow!("cron store: invalid enabled state")),
        },
    })
}

impl CronDocuments {
    /// Installs or removes an operator-owned, scope-wide enabled-job cap.
    /// Existing handles observe the stored policy. Installation refuses a cap
    /// below the existing count. All participating writers need atomic batches;
    /// unsupported backends fail closed, without a local fallback.
    ///
    /// # Errors
    /// Zero or exceeded limit, malformed admission state, unavailable atomic
    /// batches, exhausted retries, or backend failure.
    pub async fn set_max_active_jobs(&self, limit: Option<usize>) -> Result<()> {
        self.docs
            .capabilities()
            .require(Capability::Transactions)
            .map_err(storage_error)?;
        self.ensure().await?;
        for _ in 0..CAS_ATTEMPTS {
            let stored = self.admission().await?;
            let mut state: Admission = decode(stored.doc.clone())?;
            state.limit = limit;
            state.check()?;
            match self
                .docs
                .put(COUNTERS, ADMISSION, encode(state)?, stored.unchanged())
                .await
            {
                Ok(_) => return Ok(()),
                Err(error) if error.kind() == ErrorKind::Conflict => {}
                Err(error) => return Err(storage_error(error)),
            }
        }
        bail!("cron store: admission policy kept changing")
    }

    /// Initializes authoritative membership from legacy rows, fencing every
    /// snapshot row and the absent ledger in one transaction. Upgraded writers
    /// all consult the ledger, including those without a configured cap.
    async fn admission(&self) -> Result<Versioned<Value>> {
        for _ in 0..CAS_ATTEMPTS {
            if let Some(stored) = self
                .docs
                .get(COUNTERS, ADMISSION)
                .await
                .map_err(storage_error)?
            {
                let state: Admission = decode(stored.doc.clone())?;
                state.check()?;
                return Ok(stored);
            }
            let rows = self
                .docs
                .query_all(JOBS, &Query::all())
                .await
                .map_err(storage_error)?;
            let mut state = Admission::default();
            let mut ops = Vec::new();
            for row in rows {
                doc_to_job(&row)?;
                state.jobs.insert(row.id.clone(), member(&row.doc)?);
                ops.push(WriteOp::Put {
                    collection: JOBS.into(),
                    id: row.id.clone(),
                    doc: row.doc.clone(),
                    precondition: row.unchanged(),
                });
            }
            ops.push(WriteOp::Put {
                collection: COUNTERS.into(),
                id: ADMISSION.into(),
                doc: encode(state)?,
                precondition: Precondition::Absent,
            });
            match self.docs.atomic_batch(ops).await {
                Ok(_) => {}
                Err(error) if error.kind() == ErrorKind::Conflict => {}
                Err(error) => return Err(storage_error(error)),
            }
        }
        bail!("cron store: admission initialization kept changing")
    }

    /// A nontransactional backend must never bypass persisted policy.
    pub(super) async fn require_unlimited_backend(&self) -> Result<()> {
        if self
            .docs
            .get(COUNTERS, ADMISSION)
            .await
            .map_err(storage_error)?
            .is_some()
        {
            anyhow::bail!("cron store: transactions required for stored admission policy");
        }
        Ok(())
    }

    /// Commits a job plus membership and optional run writes atomically. The
    /// ledger incarnation check closes the delete/re-create version ABA gap.
    pub(super) async fn commit_job(
        &self,
        id: &str,
        previous: Option<&Versioned<Value>>,
        next: Option<Value>,
        mut extra: Vec<WriteOp>,
    ) -> Result<Vec<WriteResult>> {
        if !self.docs.capabilities().contains(Capability::Transactions) {
            // This backend cannot install admission policy, so absent policy
            // is a stable fact, not a race with another participating writer.
            self.require_unlimited_backend().await?;
            if !extra.is_empty() {
                anyhow::bail!("cron store: transactions required for atomic run writes");
            }
            let precondition = previous.map_or(Precondition::Absent, Versioned::unchanged);
            return match next {
                Some(doc) => self
                    .docs
                    .put(JOBS, id, doc, precondition)
                    .await
                    .map(|version| vec![WriteResult::Put { version }])
                    .map_err(storage_error),
                None => self
                    .docs
                    .delete(JOBS, id, precondition)
                    .await
                    .map(|removed| vec![WriteResult::Delete { removed }])
                    .map_err(storage_error),
            };
        }
        let ledger = self.admission().await?;
        let mut state: Admission = decode(ledger.doc.clone())?;
        let owns = match (previous, state.jobs.get(id)) {
            (None, None) => true,
            (Some(old), Some(current)) => current.incarnation.as_deref() == incarnation(&old.doc),
            _ => false,
        };
        if !owns {
            return Err(storage_error(
                tinystoragedrivers_core::StorageError::conflict("cron job incarnation changed"),
            ));
        }
        let current = self.docs.get(JOBS, id).await.map_err(storage_error)?;
        let precondition = match (previous, current.as_ref()) {
            (None, None) => Precondition::Absent,
            (Some(old), Some(current)) if old.doc == current.doc => current.unchanged(),
            _ => {
                return Err(storage_error(
                    tinystoragedrivers_core::StorageError::conflict("cron job changed"),
                ));
            }
        };
        let op = match next {
            Some(doc) => {
                state.jobs.insert(id.into(), member(&doc)?);
                WriteOp::Put {
                    collection: JOBS.into(),
                    id: id.into(),
                    doc,
                    precondition,
                }
            }
            None => {
                state.jobs.remove(id);
                WriteOp::Delete {
                    collection: JOBS.into(),
                    id: id.into(),
                    precondition,
                }
            }
        };
        state.check()?;
        let mut ops = vec![op];
        ops.append(&mut extra);
        ops.push(WriteOp::Put {
            collection: COUNTERS.into(),
            id: ADMISSION.into(),
            doc: encode(state)?,
            precondition: ledger.unchanged(),
        });
        self.docs.atomic_batch(ops).await.map_err(storage_error)
    }
}

/// A conflicted atomic write is retryable; all other errors fail closed.
pub(super) fn conflict(error: &anyhow::Error) -> bool {
    // `storage_error` retains the storage error as an anyhow source below.
    error
        .downcast_ref::<tinystoragedrivers_core::StorageError>()
        .is_some_and(|error| error.kind() == ErrorKind::Conflict)
}
