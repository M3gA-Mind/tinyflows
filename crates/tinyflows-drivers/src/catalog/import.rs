//! Id-preserving, write-if-absent import of records from an older store.
//!
//! The hosts that kept the catalog in `flows.db` and the drafts as JSON files
//! (the retired `tinyflows-sqlite`) move onto documents by reading those
//! records back as the catalog model and handing them to the methods below.
//! Each one stores a record under the id it already has, in the same document
//! shape the catalog's own writers produce, and only when no document with
//! that id exists: it never replaces a record the catalog already holds, so an
//! import can be repeated (after a crash part-way) or run against a scope that
//! has since been written to without clobbering anything. The return value says
//! whether this call wrote the record.
//!
//! The reading half (opening the legacy files) lives in the unpublished
//! `tinyflows-import` crate, which is where the file formats are known.

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use tinyflows_catalog::{Flow, FlowDraft, FlowRevision, FlowRun, FlowSuggestion};
use tinystoragedrivers_core::{DocumentStore, ErrorKind, Precondition};

use super::definitions::to_doc as flow_doc;
use super::drafts::{check_id, to_doc as draft_doc};
use super::lineage::{FLOW_INCARNATION, incarnation};
use super::runs::set_finished;
use super::steps::with_steps;
use super::suggestions::suggestion_doc;
use super::{
    DEFINITIONS, DRAFTS, FlowCatalogDocuments, KV, REVISIONS, RUNS, SUGGESTIONS, composite_id,
    instant_ns, set_optional,
};

/// Writes `doc` at `collection/id` only if absent. `Ok(false)` when a document
/// already held the id.
async fn put_if_absent(
    docs: &dyn DocumentStore,
    collection: &str,
    id: &str,
    doc: Value,
) -> Result<bool> {
    match docs.put(collection, id, doc, Precondition::Absent).await {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == ErrorKind::AlreadyExists => Ok(false),
        Err(error) => Err(error).with_context(|| format!("importing {collection}/{id}")),
    }
}

impl FlowCatalogDocuments {
    /// Imports a flow under its own id, keeping its timestamps, enabled flag,
    /// approval requirement and last-run summary.
    ///
    /// # Errors
    ///
    /// When the graph cannot be serialized or the backend fails.
    pub async fn import_flow(&self, flow: &Flow) -> Result<bool> {
        let docs = self.docs().await?;
        let wrote = put_if_absent(docs.as_ref(), DEFINITIONS, &flow.id, flow_doc(flow, None)?)
            .await?;
        tracing::debug!(target: "flows", flow_id = %flow.id, wrote, "[flows] import: flow");
        Ok(wrote)
    }

    /// Imports a superseded-graph revision under its own id, visible at once.
    /// The flow it belongs to must already exist (import flows first).
    ///
    /// # Errors
    ///
    /// When the flow is missing or the backend fails.
    pub async fn import_revision(&self, revision: &FlowRevision) -> Result<bool> {
        let docs = self.docs().await?;
        let flow = self.owner(&revision.flow_id, &revision.id).await?;
        let mut doc = json!({
            "flow_id": revision.flow_id,
            "graph_json": serde_json::to_string(&revision.graph)
                .context("Failed to serialize revision graph")?,
            "name": revision.name,
            "require_approval": revision.require_approval,
            "created_at": revision.created_at,
            "created_ns": instant_ns(&revision.created_at),
        });
        set_optional(&mut doc, FLOW_INCARNATION, incarnation(&flow));
        let wrote = put_if_absent(docs.as_ref(), REVISIONS, &revision.id, doc).await?;
        tracing::debug!(target: "flows", revision_id = %revision.id, wrote, "[flows] import: revision");
        Ok(wrote)
    }

    /// Imports a run record, with its steps, pending approvals, error and
    /// parking hash, under its own id. The flow it belongs to must already
    /// exist (import flows first). A run still marked live stays live; the host
    /// decides whether to reconcile it.
    ///
    /// # Errors
    ///
    /// When the flow is missing or the backend fails.
    pub async fn import_run(&self, run: &FlowRun) -> Result<bool> {
        let docs = self.docs().await?;
        let flow = self.owner(&run.flow_id, &run.id).await?;
        let mut doc = json!({
            "flow_id": run.flow_id,
            "thread_id": run.thread_id,
            "status": run.status,
            "started_at": run.started_at,
            "started_ns": instant_ns(&run.started_at),
            "steps_json": "[]",
            "pending_approvals_json": serde_json::to_string(&run.pending_approvals)
                .context("Failed to serialize flow run pending approvals")?,
        });
        doc = with_steps(&doc, &run.steps)?;
        set_finished(&mut doc, run.finished_at.as_deref());
        set_optional(&mut doc, "error", run.error.as_deref());
        set_optional(&mut doc, "graph_hash", run.graph_hash.as_deref());
        set_optional(&mut doc, FLOW_INCARNATION, incarnation(&flow));
        let wrote = put_if_absent(docs.as_ref(), RUNS, &run.id, doc).await?;
        tracing::debug!(target: "flows", run_id = %run.id, wrote, "[flows] import: run");
        Ok(wrote)
    }

    /// Imports one namespaced state value, keeping any value already stored.
    ///
    /// # Errors
    ///
    /// When the value cannot be serialized or the backend fails.
    pub async fn import_kv(&self, namespace: &str, key: &str, value: &Value) -> Result<bool> {
        let docs = self.docs().await?;
        let raw = serde_json::to_string(value).context("Failed to serialize flow state value")?;
        let id = composite_id(&[namespace, key]);
        let doc = json!({ "namespace": namespace, "key": key, "value_json": raw });
        put_if_absent(docs.as_ref(), KV, &id, doc).await
    }

    /// Imports a discovery suggestion, keeping its status and creation time.
    ///
    /// # Errors
    ///
    /// When the backend fails.
    pub async fn import_suggestion(&self, suggestion: &FlowSuggestion) -> Result<bool> {
        let docs = self.docs().await?;
        let doc = suggestion_doc(
            suggestion,
            suggestion.status.as_str(),
            &suggestion.created_at,
        )?;
        put_if_absent(docs.as_ref(), SUGGESTIONS, &suggestion.id, doc).await
    }

    /// Imports an authoring draft under its own id, keeping its timestamps.
    ///
    /// # Errors
    ///
    /// When the id is not a safe draft id or the backend fails.
    pub async fn import_draft(&self, draft: &FlowDraft) -> Result<bool> {
        check_id(&draft.id)?;
        let docs = self.docs().await?;
        put_if_absent(docs.as_ref(), DRAFTS, &draft.id, draft_doc(draft)?).await
    }

    /// The stored definition `flow_id` that record `record_id` hangs off.
    async fn owner(&self, flow_id: &str, record_id: &str) -> Result<Value> {
        let docs = self.docs().await?;
        match docs.get(DEFINITIONS, flow_id).await? {
            Some(stored) => Ok(stored.doc),
            None => bail!("cannot import {record_id}: flow '{flow_id}' does not exist"),
        }
    }
}

#[cfg(test)]
#[path = "import_tests.rs"]
mod tests;
