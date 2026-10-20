//! Bounded, host-side readers for the retired flow catalog and draft files.

use std::future::Future;
use std::io::{self, Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use tinyflows_bus::catalog::{
    LegacyDraftRecord, LegacyFlowRecord, LegacyImportKind, LegacyImportRecord,
    LegacyRevisionRecord, LegacyRunRecord, LegacyStateRecord, LegacySuggestionRecord,
    MAX_IMPORT_BATCH_RECORDS, MAX_IMPORT_RECORD_PAYLOAD_BYTES,
};

use crate::legacy_db::{LegacyDb, column_or};

fn fields(names: &[&str]) -> Vec<(String, String)> {
    names
        .iter()
        .map(|name| ((*name).to_string(), (*name).to_string()))
        .collect()
}

fn bounded_page_query(
    table: &str,
    fields: &[(String, String)],
    _size_fields: &[&str],
    order: &str,
) -> String {
    let projection = fields
        .iter()
        .map(|(expression, alias)| {
            let expression = expression
                .split_once(" AS ")
                .map_or(expression.as_str(), |(expression, _)| expression);
            format!("{expression} AS {alias}")
        })
        .collect::<Vec<_>>()
        .join(", ");
    let size = fields
        .iter()
        .map(|(expression, _)| {
            let expression = expression
                .split_once(" AS ")
                .map_or(expression.as_str(), |(expression, _)| expression);
            format!("COALESCE(octet_length(({expression})), 0)")
        })
        .collect::<Vec<_>>()
        .join(" + ");
    let guarded = fields
        .iter()
        .map(|(_, alias)| format!("CASE WHEN __bytes <= ?1 THEN {alias} END"))
        .collect::<Vec<_>>()
        .join(", ");
    let guarded = fields
        .iter()
        .map(|(_, alias)| format!("CASE WHEN ({size}) <= ?1 THEN {alias} END"))
        .collect::<Vec<_>>()
        .join(", ");
    let _ = projection;
    format!(
        "SELECT {guarded}, (({size}) > ?1) FROM {table} ORDER BY {order} LIMIT 1 OFFSET ?2"
    )
}

/// Reads legacy flow database rows and draft files as bounded ordered batches.
///
/// The caller owns source discovery, the target storage scope and migration
/// marker. Only file contents and safe file names cross the callback boundary.
/// Each callback completes before the next batch is read and sent.
pub async fn read_legacy_flow_catalog<F, Fut>(dir: &Path, mut send: F) -> Result<()>
where
    F: FnMut(Vec<LegacyImportRecord>) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let mut batches = BatchSender::default();
    if let Some(db) = LegacyDb::open(&dir.join("flows.db"))? {
        let tables = db.tables().await?;
        if tables.contains("flow_definitions") {
            let columns = db.columns("flow_definitions").await?;
            let mut fields = fields(&[
                "id",
                "name",
                "graph_json",
                "enabled",
                "created_at",
                "updated_at",
                "last_run_at",
                "last_status",
            ]);
            fields.push((
                column_or(&columns, "require_approval", "0"),
                "require_approval".into(),
            ));
            let query = bounded_page_query(
                "flow_definitions",
                &fields,
                &[
                    "id",
                    "name",
                    "graph_json",
                    "created_at",
                    "updated_at",
                    "last_run_at",
                    "last_status",
                ],
                "created_at,id",
            );
            page_rows(
                &db,
                "flow definitions",
                &query,
                fields.len(),
                LegacyImportKind::Flow,
                |row| {
                    Ok(LegacyFlowRecord {
                        id: row.get(0)?,
                        name: row.get(1)?,
                        graph_json: row.get(2)?,
                        enabled: row.get::<_, i64>(3)? != 0,
                        created_at: row.get(4)?,
                        updated_at: row.get(5)?,
                        last_run_at: row.get(6)?,
                        last_status: row.get(7)?,
                        require_approval: row.get::<_, i64>(8)? != 0,
                    })
                },
                LegacyImportRecord::Flow,
                &mut batches,
                &mut send,
            )
            .await?;
        }
        if tables.contains("flow_revisions") {
            let fields = fields(&[
                "id",
                "flow_id",
                "graph_json",
                "name",
                "require_approval",
                "created_at",
            ]);
            let query = bounded_page_query(
                "flow_revisions",
                &fields,
                &["id", "flow_id", "graph_json", "name", "created_at"],
                "created_at,id",
            );
            page_rows(
                &db,
                "flow revisions",
                &query,
                fields.len(),
                LegacyImportKind::Revision,
                |row| {
                    Ok(LegacyRevisionRecord {
                        id: row.get(0)?,
                        flow_id: row.get(1)?,
                        graph_json: row.get(2)?,
                        name: row.get(3)?,
                        require_approval: row.get::<_, i64>(4)? != 0,
                        created_at: row.get(5)?,
                    })
                },
                LegacyImportRecord::Revision,
                &mut batches,
                &mut send,
            )
            .await?;
        }
        if tables.contains("flow_runs") {
            let columns = db.columns("flow_runs").await?;
            let mut fields = fields(&[
                "id",
                "flow_id",
                "thread_id",
                "status",
                "started_at",
                "finished_at",
                "steps_json",
                "pending_approvals_json",
                "error",
            ]);
            fields.push((column_or(&columns, "graph_hash", "NULL"), "graph_hash".into()));
            let query = bounded_page_query(
                "flow_runs",
                &fields,
                &[
                    "id",
                    "flow_id",
                    "thread_id",
                    "status",
                    "started_at",
                    "finished_at",
                    "steps_json",
                    "pending_approvals_json",
                    "error",
                    "graph_hash",
                ],
                "started_at,id",
            );
            page_rows(
                &db,
                "flow runs",
                &query,
                fields.len(),
                LegacyImportKind::Run,
                |row| {
                    Ok(LegacyRunRecord {
                        id: row.get(0)?,
                        flow_id: row.get(1)?,
                        thread_id: row.get(2)?,
                        status: row.get(3)?,
                        started_at: row.get(4)?,
                        finished_at: row.get(5)?,
                        steps_json: row.get(6)?,
                        pending_approvals_json: row.get(7)?,
                        error: row.get(8)?,
                        graph_hash: row.get(9)?,
                    })
                },
                LegacyImportRecord::Run,
                &mut batches,
                &mut send,
            )
            .await?;
        }
        if tables.contains("flow_state") {
            let fields = fields(&["namespace", "key", "value"]);
            let query = bounded_page_query(
                "flow_state",
                &fields,
                &["namespace", "key", "value"],
                "namespace,key",
            );
            page_rows(
                &db,
                "flow state",
                &query,
                fields.len(),
                LegacyImportKind::State,
                |row| {
                    Ok(LegacyStateRecord {
                        namespace: row.get(0)?,
                        key: row.get(1)?,
                        value_json: row.get(2)?,
                    })
                },
                LegacyImportRecord::State,
                &mut batches,
                &mut send,
            )
            .await?;
        }
        if tables.contains("flow_suggestions") {
            let fields = fields(&[
                "id",
                "title",
                "one_liner",
                "rationale",
                "trigger_hint",
                "steps_json",
                "connections_json",
                "slugs_json",
                "build_prompt",
                "confidence",
                "status",
                "created_at",
                "source_run_id",
            ]);
            let query = bounded_page_query(
                "flow_suggestions",
                &fields,
                &[
                    "id",
                    "title",
                    "one_liner",
                    "rationale",
                    "trigger_hint",
                    "steps_json",
                    "connections_json",
                    "slugs_json",
                    "build_prompt",
                    "status",
                    "created_at",
                    "source_run_id",
                ],
                "created_at,id",
            );
            page_rows(
                &db,
                "flow suggestions",
                &query,
                fields.len(),
                LegacyImportKind::Suggestion,
                |row| {
                    Ok(LegacySuggestionRecord {
                        id: row.get(0)?,
                        title: row.get(1)?,
                        one_liner: row.get(2)?,
                        rationale: row.get(3)?,
                        trigger_hint: row.get(4)?,
                        steps_json: row.get(5)?,
                        connections_json: row.get(6)?,
                        slugs_json: row.get(7)?,
                        build_prompt: row.get(8)?,
                        confidence: row.get(9)?,
                        status: row.get(10)?,
                        created_at: row.get(11)?,
                        source_run_id: row.get(12)?,
                    })
                },
                LegacyImportRecord::Suggestion,
                &mut batches,
                &mut send,
            )
            .await?;
        }
    }
    read_drafts(&dir.join("drafts"), &mut batches, &mut send).await?;
    batches.flush(&mut send).await
}

enum BoundedRow<T> {
    Valid(T),
    Oversized,
}

async fn page_rows<T, ToRecord, F, Fut>(
    db: &LegacyDb,
    what: &'static str,
    query: &str,
    field_count: usize,
    kind: LegacyImportKind,
    decode: impl Fn(
        &tinystoragedrivers_sqlite::rusqlite::Row<'_>,
    ) -> tinystoragedrivers_sqlite::rusqlite::Result<T>
    + Send
    + Sync
    + 'static,
    to_record: ToRecord,
    batches: &mut BatchSender,
    send: &mut F,
) -> Result<()>
where
    T: Send + 'static,
    ToRecord: Fn(T) -> LegacyImportRecord,
    F: FnMut(Vec<LegacyImportRecord>) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let query = query.to_string();
    let decode = Arc::new(decode);
    let mut offset = 0_i64;
    loop {
        let statement_query = query.clone();
        let decode = Arc::clone(&decode);
        let rows = db
            .read(what, move |conn| {
                let mut statement = conn.prepare(&statement_query)?;
                let mapped = statement.query_map(
                    [MAX_IMPORT_RECORD_PAYLOAD_BYTES as i64, offset],
                    |row| {
                        if row.get::<_, bool>(field_count)? {
                            Ok(BoundedRow::Oversized)
                        } else {
                            decode(row).map(BoundedRow::Valid)
                        }
                    },
                )?;
                mapped.collect::<std::result::Result<Vec<_>, _>>()
            })
            .await?;
        if rows.is_empty() {
            break;
        }
        offset += rows.len() as i64;
        for row in rows {
            let record = match row {
                BoundedRow::Valid(row) => to_record(row),
                BoundedRow::Oversized => LegacyImportRecord::Invalid {
                    record_kind: kind,
                    identity: None,
                    message: "legacy source row exceeds the import byte limit".into(),
                },
            };
            batches.push(record, send).await?;
        }
    }
    Ok(())
}

async fn read_drafts<F, Fut>(dir: &Path, batches: &mut BatchSender, send: &mut F) -> Result<()>
where
    F: FnMut(Vec<LegacyImportRecord>) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error).with_context(|| format!("listing {}", dir.display())),
    };
    let mut paths: Vec<_> = entries
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| {
            path.is_file()
                && path.extension().is_some_and(|ext| ext == "json")
                && !path
                    .file_name()
                    .is_some_and(|name| name.to_string_lossy().starts_with('.'))
        })
        .collect();
    paths.sort();
    for path in paths {
        let file_name = path
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default();
        let file = std::fs::File::open(&path)
            .with_context(|| format!("opening {}", path.display()))?;
        let bytes = read_bounded(file, MAX_IMPORT_RECORD_PAYLOAD_BYTES)
            .with_context(|| format!("reading {}", path.display()))?;
        let record = if bytes.len() > MAX_IMPORT_RECORD_PAYLOAD_BYTES {
            LegacyImportRecord::Invalid {
                record_kind: LegacyImportKind::Draft,
                identity: Some(file_name.chars().take(128).collect()),
                message: "draft file exceeds the import byte limit".into(),
            }
        } else {
            LegacyImportRecord::Draft(LegacyDraftRecord { file_name, bytes })
        };
        batches
            .push(record, send)
            .await?;
    }
    Ok(())
}

fn read_bounded(mut reader: impl Read, limit: usize) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    reader
        .by_ref()
        .take(limit.saturating_add(1) as u64)
        .read_to_end(&mut bytes)?;
    Ok(bytes)
}

#[derive(Default)]
pub(crate) struct BatchSender {
    current: Vec<LegacyImportRecord>,
    bytes: usize,
}

impl BatchSender {
    pub(crate) async fn push<F, Fut>(
        &mut self,
        mut record: LegacyImportRecord,
        send: &mut F,
    ) -> Result<()>
    where
        F: FnMut(Vec<LegacyImportRecord>) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        let mut size = serialized_size(&record, MAX_IMPORT_RECORD_PAYLOAD_BYTES)?;
        if size.is_none() {
            let kind = record.record_kind();
            let identity = record_identity(&record);
            record = LegacyImportRecord::Invalid {
                record_kind: kind,
                identity,
                message: "record exceeds the import byte limit".into(),
            };
            size = serialized_size(&record, MAX_IMPORT_RECORD_PAYLOAD_BYTES)?;
        }
        let size = size.context("bounded invalid import record exceeds its byte limit")?;
        if !self.current.is_empty()
            && (self.current.len() >= MAX_IMPORT_BATCH_RECORDS
                || self.bytes + size > MAX_IMPORT_RECORD_PAYLOAD_BYTES)
        {
            self.flush(send).await?;
        }
        self.bytes += size;
        self.current.push(record);
        Ok(())
    }

    pub(crate) async fn flush<F, Fut>(&mut self, send: &mut F) -> Result<()>
    where
        F: FnMut(Vec<LegacyImportRecord>) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        if self.current.is_empty() {
            return Ok(());
        }
        self.bytes = 0;
        send(std::mem::take(&mut self.current)).await
    }
}

#[derive(Default)]
struct SizeCounter {
    bytes: usize,
    limit: usize,
    exceeded: bool,
}

impl Write for SizeCounter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let Some(next) = self.bytes.checked_add(bytes.len()) else {
            self.exceeded = true;
            return Err(io::Error::other("serialized import record exceeds its byte limit"));
        };
        if next > self.limit {
            self.exceeded = true;
            return Err(io::Error::other("serialized import record exceeds its byte limit"));
        }
        self.bytes = next;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn serialized_size<T: serde::Serialize>(value: &T, limit: usize) -> Result<Option<usize>> {
    let mut counter = SizeCounter {
        limit,
        ..SizeCounter::default()
    };
    match serde_json::to_writer(&mut counter, value) {
        Ok(()) => Ok(Some(counter.bytes)),
        Err(_) if counter.exceeded => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn record_identity(record: &LegacyImportRecord) -> Option<String> {
    let value = match record {
        LegacyImportRecord::Flow(row) => Some(row.id.as_str()),
        LegacyImportRecord::Revision(row) => Some(row.id.as_str()),
        LegacyImportRecord::Run(row) => Some(row.id.as_str()),
        LegacyImportRecord::State(row) => Some(row.key.as_str()),
        LegacyImportRecord::Suggestion(row) => Some(row.id.as_str()),
        LegacyImportRecord::Draft(row) => Some(row.file_name.as_str()),
        LegacyImportRecord::FileCheckpointWrite { thread_id, .. } => Some(thread_id.as_str()),
        LegacyImportRecord::SqliteCheckpointWrite(row) => Some(row.task_id.as_str()),
        LegacyImportRecord::Checkpoint { .. } | LegacyImportRecord::Invalid { .. } => None,
    };
    value.map(|identity| identity.chars().take(128).collect())
}

#[cfg(test)]
#[path = "flow_reader_tests.rs"]
mod tests;
