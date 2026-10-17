//! Owner-scoped admission and control for module-owned workflow actors.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};
use std::sync::atomic::{AtomicU8, Ordering};

use anyhow::{Context, Result, bail};
use tinybus::Connection;
use tinyflows_bus::runs::{
    CancelRunResponse, PollRun, RunControlRequest, StartRun, StartRunResponse,
};
use tinyflows::engine::CancellationToken;

mod actor;
mod events;
mod preparation;
mod cancellation;

const RUNNING: u8 = 1;
const PARKED: u8 = 2;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct OwnerKey {
    destination: String,
    object_path: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct ScopedRunId {
    owner: OwnerKey,
    run_id: String,
}

pub(super) struct ActiveRun {
    key: ScopedRunId,
    token: CancellationToken,
    phase: AtomicU8,
}

static ACTIVE: LazyLock<Mutex<HashMap<ScopedRunId, Arc<ActiveRun>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn owner(storage: &tinyflows_bus::catalog::CatalogStorage) -> OwnerKey {
    OwnerKey {
        destination: storage.callback.destination.clone(),
        object_path: storage.callback.object_path.clone(),
    }
}

fn key(storage: &tinyflows_bus::catalog::CatalogStorage, run_id: &str) -> ScopedRunId {
    ScopedRunId {
        owner: owner(storage),
        run_id: run_id.to_owned(),
    }
}

fn active(key: &ScopedRunId) -> Option<Arc<ActiveRun>> {
    ACTIVE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(key)
        .cloned()
}

fn register(key: ScopedRunId) -> std::result::Result<Arc<ActiveRun>, Arc<ActiveRun>> {
    let mut active = ACTIVE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(existing) = active.get(&key) {
        return Err(Arc::clone(existing));
    }
    let run = Arc::new(ActiveRun {
        key: key.clone(),
        token: CancellationToken::new(),
        phase: AtomicU8::new(RUNNING),
    });
    active.insert(key, Arc::clone(&run));
    Ok(run)
}

fn unregister(run: &Arc<ActiveRun>) {
    let mut active = ACTIVE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if active
        .get(&run.key)
        .is_some_and(|current| Arc::ptr_eq(current, run))
    {
        active.remove(&run.key);
    }
}

/// Validates synchronously, inserts a row and registers its cancellation token,
/// then starts exactly one actor before returning the host-chosen id.
pub(super) async fn start(
    connection: &Connection,
    request: StartRun,
) -> Result<StartRunResponse> {
    let prepared = preparation::prepare(connection, &request).await?;
    let run_key = key(&request.storage, &request.run_id);
    if let Some(existing) = active(&run_key) {
        if existing.phase.load(Ordering::Acquire) == RUNNING
            || existing.phase.load(Ordering::Acquire) == PARKED
        {
            return Ok(StartRunResponse {
                run_id: request.run_id,
                already_running: true,
            });
        }
    }
    if prepared.catalog.get_flow_run(&request.run_id).await?.is_some() {
        bail!("run id already exists in this owner scope")
    }
    let active = match register(run_key) {
        Ok(active) => active,
        Err(_) => {
            return Ok(StartRunResponse {
                run_id: request.run_id,
                already_running: true,
            });
        }
    };
    let started_at = chrono::Utc::now().to_rfc3339();
    if let Err(error) = prepared
        .catalog
        .insert_flow_run(
            &request.run_id,
            &request.context.flow_id,
            &request.run_id,
            &started_at,
        )
        .await
    {
        unregister(&active);
        return Err(error).context("reserving flow run row");
    }
    if let Err(error) = prepared.persist_snapshot(&request).await {
        let _ = prepared
            .catalog
            .finish_flow_run(
                &request.run_id,
                "failed",
                &chrono::Utc::now().to_rfc3339(),
                &[],
                &[],
                Some(&format!("could not persist run snapshot: {error}")),
                None,
            )
            .await;
        unregister(&active);
        return Err(error).context("pinning flow run execution snapshot");
    }
    tokio::spawn(actor::drive(
        connection.clone(),
        request,
        prepared,
        Arc::clone(&active),
    ));
    Ok(StartRunResponse {
        run_id: active.key.run_id.clone(),
        already_running: false,
    })
}

/// Returns a bounded event page; the durable row gates `done` and parked runs
/// remain nonterminal until a later ResumeRun slice settles them.
pub(super) async fn poll(
    connection: &Connection,
    request: RunControlRequest,
) -> Result<PollRun> {
    if request.run_id.trim().is_empty() {
        bail!("run id must not be empty")
    }
    let catalog = preparation::catalog(connection, request.storage).await?;
    let run = catalog
        .get_flow_run(&request.run_id)
        .await?
        .context("flow run does not exist in this owner scope")?;
    let values = catalog
        .flow_run_events_after(&request.run_id, request.cursor, 256)
        .await?;
    let events = values
        .into_iter()
        .map(serde_json::from_value)
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("decoding flow run events")?;
    let next_cursor = events.last().map_or(request.cursor, |event| event.seq);
    let last_seq = catalog.flow_run_event_last_seq(&request.run_id).await?;
    let terminal = matches!(
        run.status.as_str(),
        "completed" | "completed_with_warnings" | "failed" | "cancelled" | "interrupted"
    );
    Ok(PollRun {
        events,
        next_cursor,
        done: terminal && next_cursor >= last_seq,
    })
}

/// Signals a live run, or uses the row CAS to cancel a parked/orphaned run.
pub(super) async fn cancel(
    connection: &Connection,
    request: RunControlRequest,
) -> Result<CancelRunResponse> {
    if request.run_id.trim().is_empty() {
        bail!("run id must not be empty")
    }
    let scoped = key(&request.storage, &request.run_id);
    if let Some(active) = active(&scoped) {
        if active.phase.load(Ordering::Acquire) == RUNNING {
            active.token.cancel();
            return Ok(CancelRunResponse { signalled: true });
        }
    }
    cancellation::cancel_parked_or_stale(connection, request, active(&scoped)).await
}

#[cfg(test)]
#[path = "run_manager_tests.rs"]
mod tests;
