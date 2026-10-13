//! Engine checkpoints before they moved to documents: the SQLite
//! `checkpoints.db` and the JSONL files of the retired `FileCheckpointer`.
//!
//! Both hold whole serialized `Checkpoint` records, in the order they were
//! written, plus the pending writes filed under a checkpoint. They are replayed
//! into a [`DriverCheckpointer`] through its own `put` and `put_writes`, in that
//! order, so each thread's sequence is the one it had. A checkpoint the target
//! already holds (same thread, namespace and id) is skipped, and so is a write
//! it already holds, so a control-plane write the live run has since changed is
//! not put back. Per-thread leases in `checkpoints.db` are runtime coordination,
//! not data, and are not imported.

use std::collections::HashSet;
use std::path::Path;

use anyhow::{Context, Result};
use serde_json::Value;
use tinyflows::graph::ids::NodeId;
use tinyflows::graph::{Checkpoint, CheckpointConfig, Checkpointer, PendingWrite};
use tinyflows_drivers::DriverCheckpointer;

use crate::legacy_db::LegacyDb;
use crate::report::ImportReport;

/// The checkpoints in `checkpoints_db` (the file the old SQLite checkpointer was
/// opened with), copied into `checkpointer`.
///
/// # Errors
///
/// When the file exists but cannot be read, or the target store fails. A row
/// that cannot be decoded is counted in the report as invalid.
pub async fn import_sqlite_checkpoints(
    checkpoints_db: &Path,
    checkpointer: &DriverCheckpointer<Value>,
) -> Result<ImportReport> {
    let mut report = ImportReport::default();
    let Some(db) = LegacyDb::open(checkpoints_db)? else {
        return Ok(report);
    };
    let tables = db.tables().await?;
    let mut records = Vec::new();
    if tables.contains("checkpoints") {
        let rows = db
            .read("checkpoints", |conn| {
                let mut statement = conn.prepare("SELECT record FROM checkpoints ORDER BY seq")?;
                statement
                    .query_map([], |row| row.get::<_, String>(0))?
                    .collect::<Result<Vec<_>, _>>()
            })
            .await?;
        for raw in rows {
            match serde_json::from_str::<Checkpoint<Value>>(&raw) {
                Ok(checkpoint) => records.push(checkpoint),
                Err(error) => report.invalid("checkpoints", format!("a record does not decode: {error}")),
            }
        }
    }
    let mut writes = Vec::new();
    if tables.contains("checkpoint_writes") {
        let rows = db
            .read("checkpoint writes", |conn| {
                let mut statement = conn.prepare(
                    "SELECT thread_id, namespace, checkpoint_id, task_id, idx, node, channel, payload \
                     FROM checkpoint_writes ORDER BY rowid",
                )?;
                statement
                    .query_map([], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, i64>(4)?,
                            row.get::<_, String>(5)?,
                            row.get::<_, String>(6)?,
                            row.get::<_, String>(7)?,
                        ))
                    })?
                    .collect::<Result<Vec<_>, _>>()
            })
            .await?;
        for (thread_id, namespace, checkpoint_id, task_id, idx, node, channel, payload) in rows {
            let decoded = serde_json::from_str::<Vec<String>>(&namespace)
                .and_then(|namespace| Ok((namespace, serde_json::from_str::<Value>(&payload)?)));
            match decoded {
                Ok((namespace, payload)) => writes.push((
                    CheckpointConfig {
                        thread_id,
                        checkpoint_id: Some(checkpoint_id),
                        namespace,
                    },
                    PendingWrite {
                        node: NodeId::new(node),
                        task_id,
                        idx,
                        channel,
                        payload,
                    },
                )),
                Err(error) => report.invalid("writes", format!("a write does not decode: {error}")),
            }
        }
    }
    replay(checkpointer, records, writes, &mut report).await?;
    tracing::info!(
        target: "tinyflows_import",
        written = report.total_written(),
        problems = report.problems().len(),
        "[import] sqlite checkpoints"
    );
    Ok(report)
}

/// The checkpoints in `dir` (the directory the old `FileCheckpointer` was
/// rooted at: one `<thread>.jsonl` per thread and a `<thread>.writes.jsonl`
/// sidecar for its pending writes), copied into `checkpointer`.
///
/// A torn final line (a crash mid-append leaves one) is ignored, as the old
/// reader ignored it; any other line that does not decode is counted as
/// invalid.
///
/// # Errors
///
/// When the directory exists but cannot be listed or a file cannot be read, or
/// the target store fails.
pub async fn import_file_checkpoints(
    dir: &Path,
    checkpointer: &DriverCheckpointer<Value>,
) -> Result<ImportReport> {
    let mut report = ImportReport::default();
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(report),
        Err(error) => return Err(error).with_context(|| format!("listing {}", dir.display())),
    };
    let mut files: Vec<_> = entries
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.is_file())
        .collect();
    files.sort();
    let (mut records, mut writes) = (Vec::new(), Vec::new());
    for path in files {
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if let Some(stem) = name.strip_suffix(".writes.jsonl") {
            let Some(thread_id) = unescape_thread_id(stem) else {
                report.invalid("writes", format!("{name}: not a thread file name"));
                continue;
            };
            for line in lines(&path, name, "writes")? {
                match serde_json::from_str::<WriteRecord>(&line) {
                    Ok(record) => writes.push((
                        CheckpointConfig {
                            thread_id: thread_id.clone(),
                            checkpoint_id: Some(record.checkpoint_id),
                            namespace: record.namespace,
                        },
                        record.write,
                    )),
                    Err(error) => report.invalid("writes", format!("{name}: {error}")),
                }
            }
        } else if name.ends_with(".jsonl") {
            for line in lines(&path, name, "checkpoints")? {
                match serde_json::from_str::<Checkpoint<Value>>(&line) {
                    Ok(checkpoint) => records.push(checkpoint),
                    Err(error) => report.invalid("checkpoints", format!("{name}: {error}")),
                }
            }
        }
    }
    replay(checkpointer, records, writes, &mut report).await?;
    tracing::info!(
        target: "tinyflows_import",
        written = report.total_written(),
        problems = report.problems().len(),
        "[import] file checkpoints"
    );
    Ok(report)
}

/// One line of a thread's pending-writes sidecar.
#[derive(serde::Deserialize)]
struct WriteRecord {
    #[serde(default)]
    namespace: Vec<String>,
    checkpoint_id: String,
    write: PendingWrite,
}

/// The non-blank lines of `path`, without a torn final one: a last line with no
/// terminating newline that is not a complete JSON value is what a crash
/// mid-append leaves, and the old reader forgave exactly that.
fn lines(path: &Path, name: &str, kind: &'static str) -> Result<Vec<String>> {
    let text = std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let complete = text.is_empty() || text.ends_with('\n');
    let mut out: Vec<String> = text
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(str::to_string)
        .collect();
    if !complete
        && let Some(last) = out.last()
        && serde_json::from_str::<Value>(last).is_err()
    {
        tracing::warn!(target: "tinyflows_import", file = name, kind, "[import] ignoring a torn trailing line");
        out.pop();
    }
    Ok(out)
}

/// Undoes the old file name escaping: `%XX` is one byte, everything else is
/// literal. Both escape generations decode the same way.
fn unescape_thread_id(escaped: &str) -> Option<String> {
    let bytes = escaped.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = escaped.get(i + 1..i + 3)?;
            out.push(u8::from_str_radix(hex, 16).ok()?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// Puts `records` into `checkpointer` in order, then `writes`, skipping what
/// it already holds.
async fn replay(
    checkpointer: &DriverCheckpointer<Value>,
    records: Vec<Checkpoint<Value>>,
    writes: Vec<(CheckpointConfig, PendingWrite)>,
    report: &mut ImportReport,
) -> Result<()> {
    for checkpoint in records {
        let held = checkpointer
            .get_scoped(
                &checkpoint.thread_id,
                Some(&checkpoint.checkpoint_id),
                &checkpoint.namespace,
            )
            .await
            .map_err(|error| anyhow::anyhow!("{error}"))?
            .is_some();
        if held {
            report.wrote("checkpoints", false);
            continue;
        }
        checkpointer
            .put(checkpoint)
            .await
            .map_err(|error| anyhow::anyhow!("{error}"))?;
        report.wrote("checkpoints", true);
    }
    // Writes, grouped by the checkpoint they are filed under, in the order read.
    let mut groups: Vec<(CheckpointConfig, Vec<PendingWrite>)> = Vec::new();
    for (config, write) in writes {
        match groups.iter_mut().find(|(known, _)| *known == config) {
            Some((_, group)) => group.push(write),
            None => groups.push((config, vec![write])),
        }
    }
    for (config, group) in groups {
        let held: HashSet<(String, i64)> = checkpointer
            .get_writes(&config)
            .await
            .map_err(|error| anyhow::anyhow!("{error}"))?
            .iter()
            .map(|write| (write.task_id.clone(), write.idx))
            .collect();
        let (existing, fresh): (Vec<_>, Vec<_>) = group
            .into_iter()
            .partition(|write| held.contains(&(write.task_id.clone(), write.idx)));
        for _ in &existing {
            report.wrote("writes", false);
        }
        if fresh.is_empty() {
            continue;
        }
        checkpointer
            .put_writes(&config, &fresh)
            .await
            .map_err(|error| anyhow::anyhow!("{error}"))?;
        for _ in &fresh {
            report.wrote("writes", true);
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "checkpoints_tests.rs"]
mod tests;
