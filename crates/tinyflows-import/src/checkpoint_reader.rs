//! Bounded host-side readers for the retired SQLite and JSONL checkpointers.

use std::future::Future;
use std::io::{self, BufRead, BufReader};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use tinyflows_bus::catalog::{
    LegacyImportKind, LegacyImportRecord, LegacySqliteCheckpointWrite,
    MAX_IMPORT_RECORD_PAYLOAD_BYTES,
};

use crate::flow_reader::BatchSender;
use crate::legacy_db::LegacyDb;

/// Reads SQLite and file/JSONL checkpoint sources as bounded callback batches.
///
/// A torn invalid final JSONL line is ignored as the retired file reader did.
/// The source paths remain host-side and are never serialized into records.
pub async fn read_legacy_checkpoint_batches<F, Fut>(
    sqlite_path: &Path,
    file_dir: &Path,
    send: F,
) -> Result<()>
where
    F: FnMut(Vec<LegacyImportRecord>) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    read_sources(Some(sqlite_path), Some(file_dir), send).await
}

/// Reads only the retired SQLite checkpoint database as bounded raw batches.
pub async fn read_legacy_sqlite_checkpoint_batches<F, Fut>(
    sqlite_path: &Path,
    send: F,
) -> Result<()>
where
    F: FnMut(Vec<LegacyImportRecord>) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    read_sources(Some(sqlite_path), None, send).await
}

/// Reads only the retired file/JSONL checkpoint directory as bounded raw batches.
pub async fn read_legacy_file_checkpoint_batches<F, Fut>(file_dir: &Path, send: F) -> Result<()>
where
    F: FnMut(Vec<LegacyImportRecord>) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    read_sources(None, Some(file_dir), send).await
}

async fn read_sources<F, Fut>(
    sqlite_path: Option<&Path>,
    file_dir: Option<&Path>,
    mut send: F,
) -> Result<()>
where
    F: FnMut(Vec<LegacyImportRecord>) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let mut batches = BatchSender::default();
    if let Some(sqlite_path) = sqlite_path
        && let Some(db) = LegacyDb::open(sqlite_path)?
    {
        let tables = db.tables().await?;
        if tables.contains("checkpoints") {
            let mut offset = 0_i64;
            loop {
                let rows = db
                    .read("checkpoints", move |connection| {
                        let mut statement = connection.prepare(
                            "SELECT record FROM checkpoints ORDER BY seq LIMIT ?1 OFFSET ?2",
                        )?;
                        statement
                            .query_map([64_i64, offset], |row| row.get::<_, String>(0))?
                            .collect::<std::result::Result<Vec<_>, _>>()
                    })
                    .await?;
                if rows.is_empty() {
                    break;
                }
                offset += rows.len() as i64;
                for raw_json in rows {
                    batches
                        .push(LegacyImportRecord::Checkpoint { raw_json }, &mut send)
                        .await?;
                }
            }
        }
        if tables.contains("checkpoint_writes") {
            let mut offset = 0_i64;
            loop {
                let rows = db.read("checkpoint writes", move |connection| {
                    let mut statement = connection.prepare(
                        "SELECT thread_id,namespace,checkpoint_id,task_id,idx,node,channel,payload FROM checkpoint_writes ORDER BY rowid LIMIT ?1 OFFSET ?2",
                    )?;
                    let mapped = statement.query_map([64_i64, offset], |row| {
                        Ok(LegacySqliteCheckpointWrite {
                            thread_id: row.get(0)?, namespace_json: row.get(1)?, checkpoint_id: row.get(2)?,
                            task_id: row.get(3)?, idx: row.get(4)?, node: row.get(5)?, channel: row.get(6)?, payload_json: row.get(7)?,
                        })
                    })?;
                    mapped.collect::<std::result::Result<Vec<_>, _>>()
                }).await?;
                if rows.is_empty() {
                    break;
                }
                offset += rows.len() as i64;
                for row in rows {
                    batches
                        .push(LegacyImportRecord::SqliteCheckpointWrite(row), &mut send)
                        .await?;
                }
            }
        }
    }
    if let Some(file_dir) = file_dir {
        read_file_checkpoints(file_dir, &mut batches, &mut send).await?;
    }
    batches.flush(&mut send).await
}

async fn read_file_checkpoints<F, Fut>(
    dir: &Path,
    batches: &mut BatchSender,
    send: &mut F,
) -> Result<()>
where
    F: FnMut(Vec<LegacyImportRecord>) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error).with_context(|| format!("listing {}", dir.display())),
    };
    let mut files: Vec<PathBuf> = entries
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.is_file())
        .collect();
    files.sort();
    for path in files {
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if let Some(stem) = name.strip_suffix(".writes.jsonl") {
            let Some(thread_id) = unescape_thread_id(stem) else {
                batches
                    .push(
                        LegacyImportRecord::Invalid {
                            record_kind: LegacyImportKind::Write,
                            identity: Some(name.chars().take(128).collect()),
                            message: "not a valid checkpoint thread filename".into(),
                        },
                        send,
                    )
                    .await?;
                continue;
            };
            read_jsonl(
                &path,
                name,
                thread_id,
                LegacyImportKind::Write,
                batches,
                send,
            )
            .await?;
        } else if name.ends_with(".jsonl") {
            let Some(thread_id) = name.strip_suffix(".jsonl").and_then(unescape_thread_id) else {
                batches
                    .push(
                        LegacyImportRecord::Invalid {
                            record_kind: LegacyImportKind::Checkpoint,
                            identity: Some(name.chars().take(128).collect()),
                            message: "not a valid checkpoint thread filename".into(),
                        },
                        send,
                    )
                    .await?;
                continue;
            };
            read_jsonl(
                &path,
                name,
                thread_id,
                LegacyImportKind::Checkpoint,
                batches,
                send,
            )
            .await?;
        }
    }
    Ok(())
}

async fn read_jsonl<F, Fut>(
    path: &Path,
    name: &str,
    thread_id: String,
    kind: LegacyImportKind,
    batches: &mut BatchSender,
    send: &mut F,
) -> Result<()>
where
    F: FnMut(Vec<LegacyImportRecord>) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let file = std::fs::File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let mut reader = BufReader::new(file);
    while let Some((line, terminated, oversized)) = read_bounded_line(&mut reader)? {
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        if oversized {
            batches
                .push(
                    LegacyImportRecord::Invalid {
                        record_kind: kind,
                        identity: Some(thread_id.chars().take(128).collect()),
                        message: "checkpoint JSONL record exceeds the import byte limit".into(),
                    },
                    send,
                )
                .await?;
            continue;
        }
        if !terminated && serde_json::from_slice::<serde_json::Value>(&line).is_err() {
            tracing::warn!(target: "tinyflows_import", file = name, "[import] ignoring a torn trailing checkpoint line");
            continue;
        }
        let Ok(raw_json) = String::from_utf8(line) else {
            batches
                .push(
                    LegacyImportRecord::Invalid {
                        record_kind: kind,
                        identity: Some(thread_id.chars().take(128).collect()),
                        message: "checkpoint JSONL record is not UTF-8".into(),
                    },
                    send,
                )
                .await?;
            continue;
        };
        let record = match kind {
            LegacyImportKind::Checkpoint => LegacyImportRecord::Checkpoint { raw_json },
            LegacyImportKind::Write => LegacyImportRecord::FileCheckpointWrite {
                thread_id: thread_id.clone(),
                raw_json,
            },
            _ => unreachable!("only checkpoint and write files are read here"),
        };
        batches.push(record, send).await?;
    }
    Ok(())
}

fn read_bounded_line<R: BufRead>(reader: &mut R) -> io::Result<Option<(Vec<u8>, bool, bool)>> {
    let mut line = Vec::new();
    let mut saw_bytes = false;
    let mut oversized = false;
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return Ok(saw_bytes.then_some((line, false, oversized)));
        }
        saw_bytes = true;
        let newline = available.iter().position(|byte| *byte == b'\n');
        let consumed = newline.map_or(available.len(), |index| index + 1);
        let content_len = newline.map_or(consumed, |index| index);
        if !oversized {
            if line.len() + content_len > MAX_IMPORT_RECORD_PAYLOAD_BYTES {
                oversized = true;
                line.clear();
            } else {
                line.extend_from_slice(&available[..content_len]);
            }
        }
        reader.consume(consumed);
        if newline.is_some() {
            return Ok(Some((line, true, oversized)));
        }
    }
}

fn unescape_thread_id(escaped: &str) -> Option<String> {
    let bytes = escaped.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let hex = escaped.get(index + 1..index + 3)?;
            out.push(u8::from_str_radix(hex, 16).ok()?);
            index += 3;
        } else {
            out.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(out).ok()
}

#[cfg(test)]
#[path = "checkpoint_reader_tests.rs"]
mod tests;
