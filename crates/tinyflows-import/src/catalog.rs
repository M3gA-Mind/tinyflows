//! `flows.db` and the draft files: the flow catalog before it moved to
//! documents.
//!
//! `flows.db` holds five tables (`flow_definitions`, `flow_revisions`,
//! `flow_runs`, `flow_state`, `flow_suggestions`), any of which may be missing
//! from a database that never used it, and two of which gained a column over
//! time (`flow_definitions.require_approval`, `flow_runs.graph_hash`); a
//! database from before a column existed reads as if the column held its
//! default. Drafts were one JSON file each under `drafts/`.

use std::collections::HashSet;
use std::path::Path;

use anyhow::{Context, Result};
use serde_json::Value;
use tinyflows_catalog::{
    Flow, FlowDraft, FlowRevision, FlowRun, FlowRunStep, FlowSuggestion, SuggestionStatus,
};
use tinyflows_drivers::catalog::FlowCatalogDocuments;

use crate::legacy_db::{LegacyDb, column_or};
use crate::report::ImportReport;

/// The `flows.db` and drafts directory of a catalog, copied into `catalog`.
///
/// `dir` is the directory the old store was opened with: it holds `flows.db`
/// and, if drafts were ever saved, `drafts/`. Either may be absent.
///
/// # Errors
///
/// When `flows.db` exists but cannot be read, a draft directory cannot be
/// listed, or the target store fails. A record that cannot be decoded is not an
/// error; it is counted in the report as invalid.
pub async fn import_flow_catalog(dir: &Path, catalog: &FlowCatalogDocuments) -> Result<ImportReport> {
    let mut report = ImportReport::default();
    if let Some(db) = LegacyDb::open(&dir.join("flows.db"))? {
        let tables = db.tables().await?;
        let mut flows = HashSet::new();
        if tables.contains("flow_definitions") {
            flows = import_flows(&db, catalog, &mut report).await?;
        }
        if tables.contains("flow_revisions") {
            import_revisions(&db, catalog, &flows, &mut report).await?;
        }
        if tables.contains("flow_runs") {
            import_runs(&db, catalog, &flows, &mut report).await?;
        }
        if tables.contains("flow_state") {
            import_state(&db, catalog, &mut report).await?;
        }
        if tables.contains("flow_suggestions") {
            import_suggestions(&db, catalog, &mut report).await?;
        }
    }
    import_drafts(&dir.join("drafts"), catalog, &mut report).await?;
    tracing::info!(
        target: "tinyflows_import",
        written = report.total_written(),
        problems = report.problems().len(),
        "[import] flow catalog"
    );
    Ok(report)
}

struct FlowRow {
    id: String,
    name: String,
    graph_json: String,
    enabled: i64,
    created_at: String,
    updated_at: String,
    last_run_at: Option<String>,
    last_status: Option<String>,
    require_approval: i64,
}

/// A graph as the engine reads a persisted one: migrated to the current schema
/// first, so a definition saved by an older build keeps loading.
fn decode_graph(raw: &str) -> Result<tinyflows::model::WorkflowGraph> {
    let value: Value = serde_json::from_str(raw).context("graph_json is not JSON")?;
    let migrated = tinyflows::migrate::migrate(value).map_err(|error| anyhow::anyhow!("{error}"))?;
    serde_json::from_value(migrated).context("graph_json does not decode")
}

async fn import_flows(
    db: &LegacyDb,
    catalog: &FlowCatalogDocuments,
    report: &mut ImportReport,
) -> Result<HashSet<String>> {
    let have = db.columns("flow_definitions").await?;
    let select = format!(
        "SELECT id, name, graph_json, enabled, created_at, updated_at, last_run_at, last_status, {} \
         FROM flow_definitions ORDER BY created_at, id",
        column_or(&have, "require_approval", "0")
    );
    let rows = db
        .read("flow definitions", move |conn| {
            let mut statement = conn.prepare(&select)?;
            statement
                .query_map([], |row| {
                    Ok(FlowRow {
                        id: row.get(0)?,
                        name: row.get(1)?,
                        graph_json: row.get(2)?,
                        enabled: row.get(3)?,
                        created_at: row.get(4)?,
                        updated_at: row.get(5)?,
                        last_run_at: row.get(6)?,
                        last_status: row.get(7)?,
                        require_approval: row.get(8)?,
                    })
                })?
                .collect::<Result<Vec<_>, _>>()
        })
        .await?;
    let mut imported = HashSet::new();
    for row in rows {
        let graph = match decode_graph(&row.graph_json) {
            Ok(graph) => graph,
            Err(error) => {
                report.invalid("flows", format!("flow '{}': {error:#}", row.id));
                continue;
            }
        };
        let flow = Flow {
            id: row.id.clone(),
            name: row.name,
            graph,
            enabled: row.enabled != 0,
            created_at: row.created_at,
            updated_at: row.updated_at,
            last_run_at: row.last_run_at,
            last_status: row.last_status,
            require_approval: row.require_approval != 0,
        };
        report.wrote("flows", catalog.import_flow(&flow).await?);
        imported.insert(row.id);
    }
    Ok(imported)
}

async fn import_revisions(
    db: &LegacyDb,
    catalog: &FlowCatalogDocuments,
    flows: &HashSet<String>,
    report: &mut ImportReport,
) -> Result<()> {
    let rows = db
        .read("flow revisions", |conn| {
            let mut statement = conn.prepare(
                "SELECT id, flow_id, graph_json, name, require_approval, created_at \
                 FROM flow_revisions ORDER BY created_at, id",
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
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()
        })
        .await?;
    for (id, flow_id, graph_json, name, require_approval, created_at) in rows {
        if !flows.contains(&flow_id) {
            report.invalid("revisions", format!("revision '{id}': flow '{flow_id}' was not imported"));
            continue;
        }
        let graph: Value = match serde_json::from_str(&graph_json) {
            Ok(graph) => graph,
            Err(error) => {
                report.invalid("revisions", format!("revision '{id}': graph_json is not JSON: {error}"));
                continue;
            }
        };
        let revision = FlowRevision {
            id,
            flow_id,
            graph,
            name,
            require_approval: require_approval != 0,
            created_at,
        };
        report.wrote("revisions", catalog.import_revision(&revision).await?);
    }
    Ok(())
}

struct RunRow {
    id: String,
    flow_id: String,
    thread_id: String,
    status: String,
    started_at: String,
    finished_at: Option<String>,
    steps_json: String,
    pending_approvals_json: String,
    error: Option<String>,
    graph_hash: Option<String>,
}

async fn import_runs(
    db: &LegacyDb,
    catalog: &FlowCatalogDocuments,
    flows: &HashSet<String>,
    report: &mut ImportReport,
) -> Result<()> {
    let have = db.columns("flow_runs").await?;
    let select = format!(
        "SELECT id, flow_id, thread_id, status, started_at, finished_at, steps_json, \
         pending_approvals_json, error, {} FROM flow_runs ORDER BY started_at, id",
        column_or(&have, "graph_hash", "NULL")
    );
    let rows = db
        .read("flow runs", move |conn| {
            let mut statement = conn.prepare(&select)?;
            statement
                .query_map([], |row| {
                    Ok(RunRow {
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
                })?
                .collect::<Result<Vec<_>, _>>()
        })
        .await?;
    for row in rows {
        if !flows.contains(&row.flow_id) {
            report.invalid("runs", format!("run '{}': flow '{}' was not imported", row.id, row.flow_id));
            continue;
        }
        let steps: Vec<FlowRunStep> = match serde_json::from_str(&row.steps_json) {
            Ok(steps) => steps,
            Err(error) => {
                report.invalid("runs", format!("run '{}': steps are corrupt: {error}", row.id));
                continue;
            }
        };
        let pending_approvals: Vec<String> = match serde_json::from_str(&row.pending_approvals_json) {
            Ok(approvals) => approvals,
            Err(error) => {
                report.invalid("runs", format!("run '{}': pending approvals are corrupt: {error}", row.id));
                continue;
            }
        };
        let run = FlowRun {
            id: row.id,
            flow_id: row.flow_id,
            thread_id: row.thread_id,
            status: row.status,
            started_at: row.started_at,
            finished_at: row.finished_at,
            steps,
            pending_approvals,
            error: row.error,
            graph_hash: row.graph_hash,
        };
        report.wrote("runs", catalog.import_run(&run).await?);
    }
    Ok(())
}

async fn import_state(
    db: &LegacyDb,
    catalog: &FlowCatalogDocuments,
    report: &mut ImportReport,
) -> Result<()> {
    let rows = db
        .read("flow state", |conn| {
            let mut statement =
                conn.prepare("SELECT namespace, key, value FROM flow_state ORDER BY namespace, key")?;
            statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()
        })
        .await?;
    for (namespace, key, value) in rows {
        match serde_json::from_str::<Value>(&value) {
            Ok(value) => report.wrote("state", catalog.import_kv(&namespace, &key, &value).await?),
            Err(error) => report.invalid("state", format!("{namespace}/{key}: value is not JSON: {error}")),
        }
    }
    Ok(())
}

struct SuggestionRow {
    id: String,
    title: String,
    one_liner: String,
    rationale: String,
    trigger_hint: Option<String>,
    steps_json: String,
    connections_json: String,
    slugs_json: String,
    build_prompt: String,
    confidence: f64,
    status: String,
    created_at: String,
    source_run_id: Option<String>,
}

async fn import_suggestions(
    db: &LegacyDb,
    catalog: &FlowCatalogDocuments,
    report: &mut ImportReport,
) -> Result<()> {
    let rows = db
        .read("flow suggestions", |conn| {
            let mut statement = conn.prepare(
                "SELECT id, title, one_liner, rationale, trigger_hint, steps_json, connections_json, \
                 slugs_json, build_prompt, confidence, status, created_at, source_run_id \
                 FROM flow_suggestions ORDER BY created_at, id",
            )?;
            statement
                .query_map([], |row| {
                    Ok(SuggestionRow {
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
                })?
                .collect::<Result<Vec<_>, _>>()
        })
        .await?;
    for row in rows {
        let lists = (
            serde_json::from_str::<Vec<String>>(&row.steps_json),
            serde_json::from_str::<Vec<String>>(&row.connections_json),
            serde_json::from_str::<Vec<String>>(&row.slugs_json),
        );
        let (Ok(steps_outline), Ok(suggested_connections), Ok(suggested_slugs)) = lists else {
            report.invalid("suggestions", format!("suggestion '{}': a list column is corrupt", row.id));
            continue;
        };
        let suggestion = FlowSuggestion {
            id: row.id,
            title: row.title,
            one_liner: row.one_liner,
            rationale: row.rationale,
            trigger_hint: row.trigger_hint,
            steps_outline,
            suggested_connections,
            suggested_slugs,
            build_prompt: row.build_prompt,
            confidence: row.confidence,
            status: SuggestionStatus::from_str_lossy(&row.status),
            created_at: row.created_at,
            source_run_id: row.source_run_id,
        };
        report.wrote("suggestions", catalog.import_suggestion(&suggestion).await?);
    }
    Ok(())
}

async fn import_drafts(
    dir: &Path,
    catalog: &FlowCatalogDocuments,
    report: &mut ImportReport,
) -> Result<()> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error).with_context(|| format!("listing {}", dir.display())),
    };
    let mut paths: Vec<_> = entries
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        // `.<id>.json.tmp` staging files and anything else that is not a draft.
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
        let draft = std::fs::read(&path)
            .map_err(anyhow::Error::from)
            .and_then(|bytes| serde_json::from_slice::<FlowDraft>(&bytes).map_err(Into::into));
        match draft {
            Ok(draft) => match catalog.import_draft(&draft).await {
                Ok(written) => report.wrote("drafts", written),
                Err(error) => report.invalid("drafts", format!("{}: {error:#}", path.display())),
            },
            Err(error) => report.invalid("drafts", format!("{}: {error:#}", path.display())),
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "catalog_tests.rs"]
mod tests;
