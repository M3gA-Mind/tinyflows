//! Public checkpoint imports composed from bounded readers and the shared
//! semantic checkpoint writer.

use std::path::Path;
use std::sync::{Arc, Mutex};

#[cfg(test)]
use crate::checkpoint_reader::unescape_thread_id;
use anyhow::{Context, Result};
use serde_json::Value;
#[cfg(test)]
use tinyflows::graph::ids::NodeId;
#[cfg(test)]
use tinyflows::graph::{Checkpoint, CheckpointConfig, Checkpointer, PendingWrite};
use tinyflows_bus::catalog::LegacyImportRecord;
use tinyflows_drivers::DriverCheckpointer;

use crate::report::ImportReport;

/// Imports the legacy SQLite `checkpoints.db` into `checkpointer`.
pub async fn import_sqlite_checkpoints(
    checkpoints_db: &Path,
    checkpointer: &DriverCheckpointer<Value>,
) -> Result<ImportReport> {
    let report = Arc::new(Mutex::new(ImportReport::default()));
    let output = Arc::clone(&report);
    let mut send = |records: Vec<LegacyImportRecord>| {
        let output = Arc::clone(&output);
        async move {
            let response =
                crate::legacy_checkpoint::apply_legacy_checkpoint_batch(&records, checkpointer)
                    .await?;
            output
                .lock()
                .map_err(|_| anyhow::anyhow!("checkpoint import report lock poisoned"))?
                .merge_legacy_batch(response);
            Ok(())
        }
    };
    crate::checkpoint_reader::read_legacy_sqlite_checkpoint_batches(checkpoints_db, &mut send)
        .await
        .with_context(|| format!("importing checkpoints from {}", checkpoints_db.display()))?;
    drop(send);
    finish_report(report, "sqlite checkpoints")
}

/// Imports the retired `FileCheckpointer` JSONL directory.
pub async fn import_file_checkpoints(
    dir: &Path,
    checkpointer: &DriverCheckpointer<Value>,
) -> Result<ImportReport> {
    let report = Arc::new(Mutex::new(ImportReport::default()));
    let output = Arc::clone(&report);
    let mut send = |records: Vec<LegacyImportRecord>| {
        let output = Arc::clone(&output);
        async move {
            let response =
                crate::legacy_checkpoint::apply_legacy_checkpoint_batch(&records, checkpointer)
                    .await?;
            output
                .lock()
                .map_err(|_| anyhow::anyhow!("checkpoint import report lock poisoned"))?
                .merge_legacy_batch(response);
            Ok(())
        }
    };
    crate::checkpoint_reader::read_legacy_file_checkpoint_batches(dir, &mut send)
        .await
        .with_context(|| format!("importing checkpoints from {}", dir.display()))?;
    drop(send);
    finish_report(report, "file checkpoints")
}

fn finish_report(report: Arc<Mutex<ImportReport>>, source: &str) -> Result<ImportReport> {
    let report = Arc::try_unwrap(report)
        .map_err(|_| anyhow::anyhow!("checkpoint import report still has active readers"))?
        .into_inner()
        .map_err(|_| anyhow::anyhow!("checkpoint import report lock poisoned"))?;
    tracing::info!(target: "tinyflows_import", written = report.total_written(), problems = report.problems().len(), "[import] {source}");
    Ok(report)
}

#[cfg(test)]
#[path = "checkpoints_tests.rs"]
mod tests;
