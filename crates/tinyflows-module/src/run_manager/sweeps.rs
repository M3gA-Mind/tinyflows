//! Parked-run expiry and local startup recovery.

use std::sync::Arc;

use anyhow::{Context, Result};
use serde_json::Value;
use tinyflows_bus::runs::{
    BootSweep, BootSweepResponse, ExpireParked, ExpireParkedResponse, RunEvent,
};
use tinyflows_drivers::{DriverCheckpointer, catalog::FlowCatalogDocuments};
use tinystoragedrivers_core::DocumentStore;

use super::preparation::catalog;
use super::{PARKED, active, key, unregister};
use crate::catalog_documents::Documents;

const BOOT_REASON: &str = "Run interrupted by module restart; no local actor owns this run.";

pub(super) async fn expire_parked(
    connection: &tinybus::Connection,
    request: ExpireParked,
) -> Result<ExpireParkedResponse> {
    let catalog = catalog(connection, request.storage.clone()).await?;
    let now = chrono::Utc::now();
    let cutoff = (now
        - chrono::Duration::seconds(tinyflows_catalog::run_management::PARKED_RUN_TTL_SECS))
    .to_rfc3339();
    let expired = catalog
        .expire_parked_runs(
            &cutoff,
            &now.to_rfc3339(),
            &tinyflows_catalog::run_management::parked_expiry_message(
                tinyflows_catalog::run_management::PARKED_RUN_TTL_SECS,
            ),
        )
        .await?;
    for (run_id, _) in &expired {
        if let Some(run) = active(&key(&request.storage, run_id))
            && run.phase.load(std::sync::atomic::Ordering::Acquire) == PARKED
        {
            unregister(&run);
        }
        settle_terminal_event(&catalog, run_id, "cancelled", "parked run expired").await?;
        if let Some(run) = catalog.get_flow_run(run_id).await? {
            let documents: Arc<dyn DocumentStore> = Arc::new(
                Documents::new(connection.clone(), request.storage.callback.clone()).await?,
            );
            let checkpointer = DriverCheckpointer::<Value>::new(documents);
            if let Err(error) = checkpointer.delete_thread(&run.thread_id).await {
                tracing::warn!(%error, run_id, "expired run checkpoint cleanup failed");
            }
        }
    }
    Ok(ExpireParkedResponse { expired })
}

pub(super) async fn boot_sweep(
    connection: &tinybus::Connection,
    request: BootSweep,
) -> Result<BootSweepResponse> {
    if tinyflows_catalog::run_management::boot_sweep_plan(request.shared_backend)
        == tinyflows_catalog::run_management::BootSweepPlan::Nothing
    {
        return Ok(BootSweepResponse {
            interrupted: vec![],
        });
    }
    let catalog = catalog(connection, request.storage.clone()).await?;
    let floor = chrono::Utc::now().to_rfc3339();
    let candidates = catalog.list_running_run_ids(&floor).await?;
    let checkpointer_docs: Arc<dyn DocumentStore> = Arc::new(
        Documents::new(connection.clone(), request.storage.callback.clone())
            .await
            .context("opening startup checkpoint scope")?,
    );
    let checkpointer = DriverCheckpointer::<Value>::new(checkpointer_docs);
    let mut interrupted = Vec::new();
    for (run_id, _) in candidates {
        if active(&key(&request.storage, &run_id)).is_some() {
            continue;
        }
        if !catalog
            .mark_run_interrupted(&run_id, &floor, BOOT_REASON)
            .await?
        {
            continue;
        }
        if let Some(run) = catalog.get_flow_run(&run_id).await?
            && let Err(error) = checkpointer.delete_thread(&run.thread_id).await
        {
            tracing::warn!(%error, run_id, "orphaned run checkpoint cleanup failed");
        }
        settle_terminal_event(&catalog, &run_id, "interrupted", BOOT_REASON).await?;
        interrupted.push(run_id);
    }
    Ok(BootSweepResponse { interrupted })
}

async fn settle_terminal_event(
    catalog: &FlowCatalogDocuments,
    run_id: &str,
    status: &str,
    error: &str,
) -> Result<()> {
    let seq = catalog
        .flow_run_event_last_seq(run_id)
        .await?
        .saturating_add(1);
    catalog
        .append_flow_run_event(
            run_id,
            serde_json::to_value(RunEvent {
                seq,
                kind: "finished".into(),
                data: serde_json::json!({"status":status,"error":error}),
                graph_run_id: None,
            })?,
        )
        .await?;
    if !catalog.mark_flow_run_terminal_event(run_id, seq).await? {
        anyhow::bail!("terminal event cursor was not committed")
    }
    Ok(())
}
