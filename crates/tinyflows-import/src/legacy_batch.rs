//! Shared semantic writer for legacy flow and checkpoint records.

use std::collections::{BTreeMap, HashSet};

use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::{Value, json};
use tinyflows::graph::{Checkpoint, CheckpointConfig, Checkpointer, PendingWrite};
use tinyflows_bus::catalog::{
    ImportLegacyBatchResponse, LegacyImportKind, LegacyImportProblem, LegacyImportRecord,
    LegacyImportTally,
};
use tinyflows_catalog::{
    Flow, FlowDraft, FlowRevision, FlowRun, FlowRunStep, FlowSuggestion, SuggestionStatus,
};
use tinyflows_drivers::{DriverCheckpointer, catalog::FlowCatalogDocuments};

#[derive(Default)]
struct Counts {
    written: u64,
    existing: u64,
    invalid: u64,
}

/// Applies one already bounded batch through the caller's scoped catalog and checkpointer.
///
/// Source parsing stays in the host-only reader; this shared writer owns legacy
/// semantic decoding, migration, idempotent catalog writes and checkpoint replay.
pub async fn apply_legacy_batch(
    records: &[LegacyImportRecord],
    catalog: &FlowCatalogDocuments,
    checkpointer: &DriverCheckpointer<Value>,
) -> Result<ImportLegacyBatchResponse> {
    let mut counts: BTreeMap<LegacyImportKind, Counts> = BTreeMap::new();
    let mut problems = Vec::new();
    let mut imported_flows = HashSet::new();
    let mut checkpoints = Vec::new();
    let mut writes = Vec::new();

    for record in records {
        match record {
            LegacyImportRecord::Flow(row) => {
                let result = decode_flow(row).and_then(|flow| Ok((flow.clone(), flow)));
                match result {
                    Ok((flow, _)) => match catalog.import_flow(&flow).await {
                        Ok(wrote) => {
                            tally(&mut counts, LegacyImportKind::Flow, wrote);
                            imported_flows.insert(flow.id);
                        }
                        Err(error) => return Err(error),
                    },
                    Err(error) => problem(&mut counts, &mut problems, LegacyImportKind::Flow, Some(&row.id), error.to_string()),
                }
            }
            LegacyImportRecord::Revision(row) => {
                let decoded = (|| -> Result<FlowRevision> {
                    let graph = serde_json::from_str(&row.graph_json).context("graph_json is not JSON")?;
                    Ok(FlowRevision { id: row.id.clone(), flow_id: row.flow_id.clone(), graph, name: row.name.clone(), require_approval: row.require_approval, created_at: row.created_at.clone() })
                })();
                match decoded {
                    Ok(value) if imported_flows.contains(&value.flow_id) || catalog.get_flow(&value.flow_id).await?.is_some() => match catalog.import_revision(&value).await {
                        Ok(wrote) => tally(&mut counts, LegacyImportKind::Revision, wrote),
                        Err(error) => return Err(error),
                    },
                    Ok(value) => problem(&mut counts, &mut problems, LegacyImportKind::Revision, Some(&value.id), "owner flow was not imported".into()),
                    Err(error) => problem(&mut counts, &mut problems, LegacyImportKind::Revision, Some(&row.id), error.to_string()),
                }
            }
            LegacyImportRecord::Run(row) => {
                let decoded = (|| -> Result<FlowRun> {
                    Ok(FlowRun {
                        id: row.id.clone(), flow_id: row.flow_id.clone(), thread_id: row.thread_id.clone(), status: row.status.clone(),
                        started_at: row.started_at.clone(), finished_at: row.finished_at.clone(),
                        steps: serde_json::from_str::<Vec<FlowRunStep>>(&row.steps_json).context("run steps are corrupt")?,
                        pending_approvals: serde_json::from_str(&row.pending_approvals_json).context("pending approvals are corrupt")?,
                        error: row.error.clone(), graph_hash: row.graph_hash.clone(),
                    })
                })();
                match decoded {
                    Ok(value) if imported_flows.contains(&value.flow_id) || catalog.get_flow(&value.flow_id).await?.is_some() => match catalog.import_run(&value).await {
                        Ok(wrote) => tally(&mut counts, LegacyImportKind::Run, wrote),
                        Err(error) => return Err(error),
                    },
                    Ok(value) => problem(&mut counts, &mut problems, LegacyImportKind::Run, Some(&value.id), "owner flow was not imported".into()),
                    Err(error) => problem(&mut counts, &mut problems, LegacyImportKind::Run, Some(&row.id), error.to_string()),
                }
            }
            LegacyImportRecord::State(row) => match serde_json::from_str::<Value>(&row.value_json) {
                Ok(value) => tally(&mut counts, LegacyImportKind::State, catalog.import_kv(&row.namespace, &row.key, &value).await?),
                Err(error) => problem(&mut counts, &mut problems, LegacyImportKind::State, Some(&row.key), error.to_string()),
            },
            LegacyImportRecord::Suggestion(row) => {
                let decoded = (|| -> Result<FlowSuggestion> {
                    Ok(FlowSuggestion {
                        id: row.id.clone(), title: row.title.clone(), one_liner: row.one_liner.clone(), rationale: row.rationale.clone(), trigger_hint: row.trigger_hint.clone(),
                        steps_outline: serde_json::from_str(&row.steps_json).context("suggestion steps are corrupt")?,
                        suggested_connections: serde_json::from_str(&row.connections_json).context("suggestion connections are corrupt")?,
                        suggested_slugs: serde_json::from_str(&row.slugs_json).context("suggestion slugs are corrupt")?,
                        build_prompt: row.build_prompt.clone(), confidence: row.confidence, status: SuggestionStatus::from_str_lossy(&row.status), created_at: row.created_at.clone(), source_run_id: row.source_run_id.clone(),
                    })
                })();
                match decoded {
                    Ok(value) => tally(&mut counts, LegacyImportKind::Suggestion, catalog.import_suggestion(&value).await?),
                    Err(error) => problem(&mut counts, &mut problems, LegacyImportKind::Suggestion, Some(&row.id), error.to_string()),
                }
            }
            LegacyImportRecord::Draft(row) => match serde_json::from_slice::<FlowDraft>(&row.bytes) {
                Ok(value) => tally(&mut counts, LegacyImportKind::Draft, catalog.import_draft(&value).await?),
                Err(error) => problem(&mut counts, &mut problems, LegacyImportKind::Draft, Some(&row.file_name), error.to_string()),
            },
            LegacyImportRecord::Checkpoint { raw_json } => match serde_json::from_str::<Checkpoint<Value>>(raw_json) {
                Ok(value) => checkpoints.push(value),
                Err(error) => problem(&mut counts, &mut problems, LegacyImportKind::Checkpoint, None, error.to_string()),
            },
            LegacyImportRecord::SqliteCheckpointWrite(row) => {
                let decoded = (|| -> Result<_> {
                    Ok((CheckpointConfig { thread_id: row.thread_id.clone(), checkpoint_id: Some(row.checkpoint_id.clone()), namespace: serde_json::from_str(&row.namespace_json).context("write namespace is corrupt")? }, PendingWrite { node: tinyflows::graph::ids::NodeId::new(row.node.clone()), task_id: row.task_id.clone(), idx: row.idx, channel: row.channel.clone(), payload: serde_json::from_str(&row.payload_json).context("write payload is corrupt")? }))
                })();
                match decoded {
                    Ok(value) => writes.push((value.0, value.1)),
                    Err(error) => problem(&mut counts, &mut problems, LegacyImportKind::Write, Some(&row.task_id), error.to_string()),
                }
            }
            LegacyImportRecord::FileCheckpointWrite { thread_id, raw_json } => match serde_json::from_str::<FileWrite>(raw_json) {
                Ok(row) => writes.push((CheckpointConfig { thread_id: thread_id.clone(), checkpoint_id: Some(row.checkpoint_id), namespace: row.namespace }, row.write)),
                Err(error) => problem(&mut counts, &mut problems, LegacyImportKind::Write, Some(thread_id), error.to_string()),
            },
            LegacyImportRecord::Invalid { record_kind, identity, message } => problem(&mut counts, &mut problems, *record_kind, identity.as_deref(), message.clone()),
        }
    }
    replay(checkpointer, checkpoints, writes, &mut counts).await?;
    Ok(ImportLegacyBatchResponse {
        tallies: counts.into_iter().map(|(kind, count)| LegacyImportTally { kind, written: count.written, existing: count.existing, invalid: count.invalid }).collect(),
        problems,
    })
}

fn decode_flow(row: &tinyflows_bus::catalog::LegacyFlowRecord) -> Result<Flow> {
    let raw: Value = serde_json::from_str(&row.graph_json).context("graph_json is not JSON")?;
    let migrated = tinyflows::migrate::migrate(raw).map_err(|error| anyhow::anyhow!("{error}"))?;
    let graph = serde_json::from_value(migrated).context("graph_json does not decode")?;
    Ok(Flow { id: row.id.clone(), name: row.name.clone(), graph, enabled: row.enabled, created_at: row.created_at.clone(), updated_at: row.updated_at.clone(), last_run_at: row.last_run_at.clone(), last_status: row.last_status.clone(), require_approval: row.require_approval })
}

#[derive(Deserialize)]
struct FileWrite { #[serde(default)] namespace: Vec<String>, checkpoint_id: String, write: PendingWrite }

fn tally(counts: &mut BTreeMap<LegacyImportKind, Counts>, kind: LegacyImportKind, wrote: bool) {
    let count = counts.entry(kind).or_default();
    if wrote { count.written += 1 } else { count.existing += 1 }
}

fn problem(counts: &mut BTreeMap<LegacyImportKind, Counts>, problems: &mut Vec<LegacyImportProblem>, kind: LegacyImportKind, identity: Option<&str>, message: String) {
    counts.entry(kind).or_default().invalid += 1;
    problems.push(LegacyImportProblem { kind, identity: identity.map(str::to_owned), message: message.chars().take(240).collect() });
}

async fn replay(
    checkpointer: &DriverCheckpointer<Value>,
    checkpoints: Vec<Checkpoint<Value>>,
    writes: Vec<(CheckpointConfig, PendingWrite)>,
    counts: &mut BTreeMap<LegacyImportKind, Counts>,
) -> Result<()> {
    for checkpoint in checkpoints {
        let held = checkpointer.get_scoped(&checkpoint.thread_id, Some(&checkpoint.checkpoint_id), &checkpoint.namespace).await.map_err(|error| anyhow::anyhow!("{error}"))?.is_some();
        if held { tally(counts, LegacyImportKind::Checkpoint, false); continue; }
        checkpointer.put(checkpoint).await.map_err(|error| anyhow::anyhow!("{error}"))?;
        tally(counts, LegacyImportKind::Checkpoint, true);
    }
    let mut groups: Vec<(CheckpointConfig, Vec<PendingWrite>)> = Vec::new();
    for (config, write) in writes {
        match groups.iter_mut().find(|(known, _)| *known == config) { Some((_, group)) => group.push(write), None => groups.push((config, vec![write])) }
    }
    for (config, group) in groups {
        let held: HashSet<(String, i64)> = checkpointer.get_writes(&config).await.map_err(|error| anyhow::anyhow!("{error}"))?.iter().map(|write| (write.task_id.clone(), write.idx)).collect();
        let (existing, fresh): (Vec<_>, Vec<_>) = group.into_iter().partition(|write| held.contains(&(write.task_id.clone(), write.idx)));
        for _ in existing { tally(counts, LegacyImportKind::Write, false); }
        if !fresh.is_empty() {
            checkpointer.put_writes(&config, &fresh).await.map_err(|error| anyhow::anyhow!("{error}"))?;
            for _ in fresh { tally(counts, LegacyImportKind::Write, true); }
        }
    }
    Ok(())
}
