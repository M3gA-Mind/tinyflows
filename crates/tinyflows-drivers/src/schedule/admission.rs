//! Shared admission state, transacted with job writes to fence policy races.
use super::codec::{doc_to_job, incarnation};
use super::{CAS_ATTEMPTS, COUNTERS, CronDocuments, JOBS, storage_error};
use anyhow::{Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tinystoragedrivers_core::{
    DocumentStoreExt, ErrorKind, Precondition, Query, Versioned, WriteOp, WriteResult,
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
impl Admission {
    fn check(&self) -> Result<()> {
        if let Some(limit) = self.limit {
            if limit == 0 || self.jobs.values().filter(|member| member.enabled).count() > limit {
                bail!("cron store: active job limit exceeded");
            }
        }
        Ok(())
    }
}
fn member(doc: &Value) -> Result<Member> {
    Ok(Member {
        incarnation: incarnation(doc).map(str::to_owned),
        enabled: doc
            .get("enabled")
            .and_then(Value::as_bool)
            .ok_or_else(|| anyhow!("cron store: invalid enabled state"))?,
    })
}

impl CronDocuments {
    /// Installs or removes an operator-owned, scope-wide enabled-job cap.
    /// Existing handles observe the stored policy. Installation refuses a cap
    /// below the existing count. All participating writers need atomic batches;
    /// unsupported backends fail closed, without a local fallback.
    pub async fn set_max_active_jobs(&self, limit: Option<usize>) -> Result<()> {
        self.ensure().await?;
        for _ in 0..CAS_ATTEMPTS {
            let stored = self.admission().await?;
            let mut state: Admission = serde_json::from_value(stored.doc.clone())?;
            state.limit = limit;
            state.check()?;
            match self
                .docs
                .put(
                    COUNTERS,
                    ADMISSION,
                    serde_json::to_value(state)?,
                    stored.unchanged(),
                )
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
                let state: Admission = serde_json::from_value(stored.doc.clone())?;
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
                doc: serde_json::to_value(state)?,
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

    /// Commits a job plus membership and optional run writes atomically. The
    /// ledger incarnation check closes the delete/re-create version ABA gap.
    pub(super) async fn commit_job(
        &self,
        id: &str,
        previous: Option<&Versioned<Value>>,
        next: Option<Value>,
        mut extra: Vec<WriteOp>,
    ) -> Result<Vec<WriteResult>> {
        let ledger = self.admission().await?;
        let mut state: Admission = serde_json::from_value(ledger.doc.clone())?;
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
        let precondition = previous.map_or(Precondition::Absent, Versioned::unchanged);
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
            doc: serde_json::to_value(state)?,
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
