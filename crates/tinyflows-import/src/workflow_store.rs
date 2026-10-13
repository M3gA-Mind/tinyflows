//! The retired `FileWorkflowStore`: workflows as JSON documents, with their
//! runs, journal, proposals and revisions kept beside them.
//!
//! Where each lives is the old store's configuration, not something the files
//! say, so [`FileStoreLayout`] names the directories; [`FileStoreLayout::with_state`]
//! and [`FileStoreLayout::discover`] derive them the way the old constructors
//! did. Only the write layer's definitions are imported: the lower layers of a
//! layered store were repository-provided defaults, authored files a host
//! loads from the repository itself, not state the store owned.

use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tinyflows::store::types::{
    RunRecord, WorkflowNote, WorkflowProposal, WorkflowRecord, WorkflowRevision,
};
use tinyflows::store::parse_workflow;
use tinyflows_drivers::store::DriverWorkflowStore;

use crate::report::ImportReport;

/// Where an old `FileWorkflowStore` kept each kind of record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileStoreLayout {
    /// The write directory: one `<id>.json` document per workflow.
    pub definitions: PathBuf,
    /// One `<run id>.json` per run record.
    pub runs: PathBuf,
    /// One `<workflow id>.json` per workflow: its notes, as a JSON array.
    pub journal: PathBuf,
    /// One `<proposal id>.json` per proposal.
    pub proposals: PathBuf,
    /// Directories of superseded copies, each holding
    /// `<workflow id>/<revision id>.json`. More than one when a store was
    /// upgraded from one that kept them beside the definitions.
    pub revisions: Vec<PathBuf>,
}

impl FileStoreLayout {
    /// The layout of `FileWorkflowStore::with_state(vec![definitions], state_dir)`:
    /// runs, journal and proposals under `state_dir`, revisions under the
    /// definition state the write directory hashes to, and any older
    /// `.revisions` kept beside the definitions.
    pub fn with_state(definitions: impl Into<PathBuf>, state_dir: &Path) -> Self {
        let definitions = definitions.into();
        Self {
            runs: state_dir.join("runs"),
            journal: state_dir.join("journal"),
            proposals: state_dir.join("proposals"),
            revisions: revisions_dirs(state_dir, &definitions),
            definitions,
        }
    }

    /// The layout of `FileWorkflowStore::discover(home, cwd, _)`: definitions in
    /// `<home>/workflows`, definition state under `<home>/state/workflows`, and
    /// the runs, journal and proposals of the workspace `cwd` under that
    /// directory's `scopes/<workspace digest>`.
    pub fn discover(home: &Path, cwd: &Path) -> Self {
        let state_root = home.join("state").join("workflows");
        let definitions = home.join("workflows");
        let run_state = state_root.join("scopes").join(workspace_scope(cwd));
        Self {
            runs: run_state.join("runs"),
            journal: run_state.join("journal"),
            proposals: run_state.join("proposals"),
            revisions: revisions_dirs(&state_root, &definitions),
            definitions,
        }
    }
}

/// The current and legacy places a write directory's revisions can be.
fn revisions_dirs(state_root: &Path, definitions: &Path) -> Vec<PathBuf> {
    vec![
        definition_state_dir(state_root, definitions).join("revisions"),
        definitions.join(".revisions"),
    ]
}

/// The state the old store shared between every store writing one catalog:
/// `<root>/definitions/<sha256 of the write directory's canonical path>`.
fn definition_state_dir(state_root: &Path, write_dir: &Path) -> PathBuf {
    let identity = canonical_path_identity(&absolute(write_dir));
    let scope = hex::encode(Sha256::digest(identity.as_os_str().as_encoded_bytes()));
    state_root.join("definitions").join(scope)
}

/// The directory name that identified one workspace: the first sixteen hex
/// characters of the digest of its canonical path.
fn workspace_scope(workspace: &Path) -> String {
    let identity = std::fs::canonicalize(workspace).unwrap_or_else(|_| absolute(workspace));
    let digest = hex::encode(Sha256::digest(identity.to_string_lossy().as_bytes()));
    digest[..16].to_string()
}

fn absolute(path: &Path) -> PathBuf {
    if path.is_absolute() {
        return path.to_path_buf();
    }
    std::env::current_dir()
        .unwrap_or_else(|_| PathBuf::from("."))
        .join(path)
}

/// Resolves existing symlinks while keeping lexical meaning for parts that do
/// not exist yet, as the old store did to name a catalog.
fn canonical_path_identity(path: &Path) -> PathBuf {
    let mut resolved = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                if resolved.exists() {
                    resolved = std::fs::canonicalize(&resolved).unwrap_or(resolved);
                }
                resolved.pop();
            }
            other => {
                resolved.push(other.as_os_str());
                if resolved.exists() {
                    resolved = std::fs::canonicalize(&resolved).unwrap_or(resolved);
                }
            }
        }
    }
    resolved
}

/// A revision file: when the copy stopped being current, and the copy.
#[derive(Deserialize)]
struct StoredRevision {
    superseded_at: u64,
    record: WorkflowRecord,
}

/// The workflows, revisions, runs, journals and proposals under `layout`,
/// copied into `store`.
///
/// # Errors
///
/// When a directory exists but cannot be listed, a file cannot be read, or the
/// target store fails. A file that cannot be parsed, or a workflow the engine
/// would not compile, is counted in the report as invalid.
pub async fn import_workflow_store(
    layout: &FileStoreLayout,
    store: &DriverWorkflowStore,
) -> Result<ImportReport> {
    let mut report = ImportReport::default();

    for (path, id) in json_files(&layout.definitions)? {
        let text = std::fs::read_to_string(&path)
            .with_context(|| format!("reading {}", path.display()))?;
        match parse_workflow(&text, &id) {
            Ok(record) => match store.import_workflow(&record).await {
                Ok(written) => report.wrote("workflows", written),
                Err(tinyflows::store::types::WorkflowError::Storage(message)) => {
                    anyhow::bail!("storage: {message}")
                }
                Err(error) => report.invalid("workflows", format!("{}: {error}", path.display())),
            },
            Err(error) => report.invalid("workflows", format!("{}: {error}", path.display())),
        }
    }

    for revisions in &layout.revisions {
        let Some(workflows) = list_dir(revisions)? else {
            continue;
        };
        for workflow_dir in workflows.into_iter().filter(|path| path.is_dir()) {
            for (path, id) in json_files(&workflow_dir)? {
                let Some(stored) = read_json::<StoredRevision>(&path, "revisions", &mut report)? else {
                    continue;
                };
                let revision = WorkflowRevision {
                    id,
                    superseded_at: stored.superseded_at,
                    record: stored.record,
                };
                report.wrote("revisions", store.import_revision(&revision).await?);
            }
        }
    }

    for (path, _) in json_files(&layout.runs)? {
        if let Some(run) = read_json::<RunRecord>(&path, "runs", &mut report)? {
            report.wrote("runs", store.import_run(&run).await?);
        }
    }

    for (path, workflow_id) in json_files(&layout.journal)? {
        let Some(notes) = read_json::<Vec<WorkflowNote>>(&path, "notes", &mut report)? else {
            continue;
        };
        let offered = notes.iter().filter(|n| n.workflow_id == workflow_id).count();
        let added = store.import_notes(&workflow_id, &notes).await?;
        for _ in 0..added {
            report.wrote("notes", true);
        }
        for _ in added..offered {
            report.wrote("notes", false);
        }
        for _ in offered..notes.len() {
            report.invalid("notes", format!("{}: a note belongs to another workflow", path.display()));
        }
    }

    for (path, _) in json_files(&layout.proposals)? {
        if let Some(proposal) = read_json::<WorkflowProposal>(&path, "proposals", &mut report)? {
            report.wrote("proposals", store.import_proposal(&proposal).await?);
        }
    }

    tracing::info!(
        target: "tinyflows_import",
        written = report.total_written(),
        problems = report.problems().len(),
        "[import] workflow store"
    );
    Ok(report)
}

/// The entries of `dir`, sorted; `None` when it does not exist.
fn list_dir(dir: &Path) -> Result<Option<Vec<PathBuf>>> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).with_context(|| format!("listing {}", dir.display())),
    };
    let mut paths: Vec<PathBuf> = entries
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .collect();
    paths.sort();
    Ok(Some(paths))
}

/// Every `<stem>.json` file directly in `dir`, with its stem, sorted. A missing
/// directory is the normal state of a store that never held that record kind.
fn json_files(dir: &Path) -> Result<Vec<(PathBuf, String)>> {
    Ok(list_dir(dir)?
        .unwrap_or_default()
        .into_iter()
        .filter(|path| path.is_file() && path.extension().is_some_and(|ext| ext.eq_ignore_ascii_case("json")))
        .filter_map(|path| {
            let stem = path.file_stem()?.to_str()?.to_string();
            Some((path, stem))
        })
        .collect())
}

/// Reads and parses one file; an unparseable one is counted, not an error.
fn read_json<T: for<'de> Deserialize<'de>>(
    path: &Path,
    kind: &'static str,
    report: &mut ImportReport,
) -> Result<Option<T>> {
    let body = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    match serde_json::from_slice(&body) {
        Ok(value) => Ok(Some(value)),
        Err(error) => {
            report.invalid(kind, format!("{}: {error}", path.display()));
            Ok(None)
        }
    }
}

#[cfg(test)]
#[path = "workflow_store_tests.rs"]
mod tests;
