//! One owner-and-flow-scoped critical section for run settlement of `dedup`
//! node state.

use tinyflows::{
    model::{NodeKind, WorkflowGraph},
    nodes::control_flow::dedup_settle,
};
use tinyflows_bus::runs::RunContext;
use tinyflows_drivers::catalog::{FlowCatalogDocuments, FlowStateDocuments};

/// Holds the per-flow settlement lease until the caller finishes all terminal
/// cleanup (checkpoint deletion and terminal event persistence).
pub(super) struct DedupSettlementScope {
    flow_id: String,
    run_id: String,
    node_ids: Vec<String>,
}

impl DedupSettlementScope {
    /// Acquires the settlement scope for the exact CatalogStorage owner and
    /// the run's flow. Graphs without a `dedup` node require no lease.
    pub(super) async fn acquire(
        _storage: &tinyflows_bus::catalog::CatalogStorage,
        context: &RunContext,
        graph: &WorkflowGraph,
        run_id: &str,
    ) -> Self {
        let node_ids = graph
            .nodes
            .iter()
            .filter(|node| node.kind == NodeKind::Dedup)
            .map(|node| node.id.clone())
            .collect();
        Self {
            flow_id: context.flow_id.clone(),
            run_id: run_id.to_owned(),
            node_ids,
        }
    }

    /// Settles every dedup node against the module's real Catalog-backed
    /// FlowState. The scope remains held until the caller drops `self`.
    pub(super) async fn settle(&self, catalog: FlowCatalogDocuments, success: bool) {
        if self.node_ids.is_empty() {
            return;
        }
        let namespace = format!("flow:{}", self.flow_id);
        let node_ids = self.node_ids.clone();
        let run_id = self.run_id.clone();
        match tokio::task::spawn_blocking(move || {
            let state = FlowStateDocuments::new(catalog, namespace);
            for node_id in node_ids {
                let result = dedup_settle::settle(&state, &node_id, success);
                tracing::debug!(run_id, node_id, ?result, "settled dedup node state");
            }
        })
        .await
        {
            Ok(()) => {}
            Err(error) => {
                tracing::error!(%error, run_id = %self.run_id, "dedup settlement worker failed")
            }
        }
    }
}

#[cfg(test)]
#[path = "dedup_settlement_tests.rs"]
mod tests;
