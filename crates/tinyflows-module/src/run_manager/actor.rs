//! Execution, persistence and settlement for a single admitted run.

use std::sync::Arc;
use std::time::Duration;
use std::future::Future;

use anyhow::{Context, Result};
use serde_json::{Value, json};
use tinyflows::{
    engine::{
        Checkpointer as _, RunInput, resume_cancellable_with_checkpointer_journaled_observed,
        run_cancellable_with_checkpointer_journaled_observed,
    },
    nodes::control_flow::dedup_settle,
};
use tinyflows_bus::runs::{ResumeRun, RunContext, RunEvent, RunParked, StartRun};
use tinyflows_catalog::{FlowRunStep, run_summary};
use tinyflows_drivers::catalog::{FlowCatalogDocuments, FlowStateDocuments};
use tokio::time::sleep;

use super::events::{EventWriter, Observer};
use super::preparation::Prepared;
use super::{ActiveRun, PARKED, unregister};
use crate::caps::{CapsHostPorts, RunScope};

const STOPPED_ACTOR_REASON: &str = "Run interrupted because its module actor stopped before settlement.";

/// A drop guard for the module-owned actor task. Its cleanup is scheduled when
/// the future is constructed, outside the future body, so aborting the task
/// before its first poll still retains the actor's lease and recovery work.
struct ActorRecoveryGuard {
    catalog: FlowCatalogDocuments,
    checkpointer: DriverCheckpointer<Value>,
    active: Arc<ActiveRun>,
    run_id: String,
    lease: String,
    armed: bool,
}

impl ActorRecoveryGuard {
    fn new(
        prepared: &Prepared,
        request: &StartRun,
        active: Arc<ActiveRun>,
    ) -> Self {
        Self {
            catalog: prepared.catalog.clone(),
            checkpointer: prepared.checkpointer.clone(),
            run_id: request.run_id.clone(),
            lease: active.lease.clone(),
            active,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ActorRecoveryGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            tracing::warn!(
                target: "workflows",
                run_id = %self.run_id,
                "actor stopped without a Tokio runtime; boot recovery will reconcile the run"
            );
            unregister(&self.active);
            return;
        };

        let recovery = ActorRecovery {
            catalog: self.catalog.clone(),
            checkpointer: self.checkpointer.clone(),
            active: Arc::clone(&self.active),
            run_id: self.run_id.clone(),
            lease: self.lease.clone(),
        };
        let active_cleanup = ActiveCleanupGuard(Arc::clone(&self.active));
        runtime.spawn(async move {
            let _active_cleanup = active_cleanup;
            recovery.reconcile().await;
        });
    }
}

/// Owns the active registry entry until best-effort recovery completes. The
/// identity-aware unregister protects a replacement actor if one has already
/// registered for this owner/run key.
struct ActiveCleanupGuard(Arc<ActiveRun>);

impl Drop for ActiveCleanupGuard {
    fn drop(&mut self) {
        unregister(&self.0);
    }
}

struct ActorRecovery {
    catalog: FlowCatalogDocuments,
    checkpointer: DriverCheckpointer<Value>,
    active: Arc<ActiveRun>,
    run_id: String,
    lease: String,
}

impl ActorRecovery {
    async fn reconcile(self) {
        let snapshot = match self.catalog.flow_run_snapshot(&self.run_id).await {
            Ok(Some(snapshot))
                if snapshot.get("lease").and_then(Value::as_str) == Some(&self.lease) =>
            {
                snapshot
            }
            Ok(_) => return,
            Err(error) => {
                tracing::warn!(
                    target: "workflows",
                    run_id = %self.run_id,
                    %error,
                    "actor recovery could not read its execution snapshot"
                );
                return;
            }
        };
        let now = chrono::Utc::now().to_rfc3339();
        let interrupted = match self
            .catalog
            .interrupt_running_flow_run_for_lease(
                &self.run_id,
                &self.lease,
                &now,
                STOPPED_ACTOR_REASON,
            )
            .await
        {
            Ok(interrupted) => interrupted,
            Err(error) => {
                tracing::warn!(
                    target: "workflows",
                    run_id = %self.run_id,
                    %error,
                    "actor recovery could not settle its leased run"
                );
                return;
            }
        };
        if !interrupted {
            return;
        }
        self.active
            .phase
            .store(super::TERMINAL, std::sync::atomic::Ordering::Release);
        super::record_settled_summary(&self.catalog, &self.run_id, "interrupted").await;

        if let Some((graph, context)) = recovery_graph_context(&snapshot) {
            settle_dedup(
                self.catalog.clone(),
                &context,
                &graph,
                false,
                &self.run_id,
            )
            .await;
        } else {
            tracing::warn!(
                target: "workflows",
                run_id = %self.run_id,
                "actor recovery snapshot could not be decoded for dedup cleanup"
            );
        }
        delete_checkpoints(&self.checkpointer, &self.run_id, &self.run_id).await;
        self.append_interrupted_event().await;
    }

    async fn append_interrupted_event(&self) {
        let seq = match self.catalog.flow_run_event_last_seq(&self.run_id).await {
            Ok(seq) => seq.saturating_add(1),
            Err(error) => {
                tracing::warn!(
                    target: "workflows",
                    run_id = %self.run_id,
                    %error,
                    "actor recovery could not read the terminal event cursor"
                );
                return;
            }
        };
        let event = RunEvent {
            seq,
            kind: "finished".into(),
            data: match serde_json::to_value(events::finished_payload(
                "interrupted",
                Some(STOPPED_ACTOR_REASON.to_owned()),
                None,
                None,
                false,
            )) {
                Ok(event) => event,
                Err(error) => {
                    tracing::error!(
                        target: "workflows",
                        run_id = %self.run_id,
                        %error,
                        "actor recovery could not encode its terminal event"
                    );
                    return;
                }
            },
            graph_run_id: None,
        };
        match self
            .catalog
            .append_flow_run_event(&self.run_id, serde_json::to_value(event).unwrap_or(Value::Null))
            .await
        {
            Ok(()) => match self
                .catalog
                .mark_flow_run_terminal_event(&self.run_id, seq)
                .await
            {
                Ok(true) => {}
                Ok(false) => tracing::error!(
                    target: "workflows",
                    run_id = %self.run_id,
                    "actor recovery terminal event cursor was not committed"
                ),
                Err(error) => tracing::error!(
                    target: "workflows",
                    run_id = %self.run_id,
                    %error,
                    "actor recovery could not commit its terminal event cursor"
                ),
            },
            Err(error) => tracing::error!(
                target: "workflows",
                run_id = %self.run_id,
                %error,
                "actor recovery could not persist its terminal event"
            ),
        }
    }
}

fn recovery_graph_context(
    snapshot: &Value,
) -> Option<(tinyflows::model::WorkflowGraph, RunContext)> {
    let graph = tinyflows::migrate::deserialize_graph(snapshot.get("graph")?.clone()).ok()?;
    let context = serde_json::from_value(snapshot.get("context")?.clone()).ok()?;
    Some((graph, context))
}

pub(super) fn drive(
    connection: tinybus::Connection,
    request: StartRun,
    prepared: Prepared,
    active: Arc<ActiveRun>,
    admitted: tokio::sync::oneshot::Receiver<()>,
) -> impl Future<Output = ()> + Send + 'static {
    let recovery = ActorRecoveryGuard::new(&prepared, &request, Arc::clone(&active));
    async move {
        let mut recovery = recovery;
        drive_inner(
            connection,
            request,
            prepared,
            active,
            admitted,
            Continuation::Start,
        )
        .await;
        recovery.disarm();
    }
}

pub(super) fn drive_resume(
    connection: tinybus::Connection,
    request: StartRun,
    prepared: Prepared,
    active: Arc<ActiveRun>,
    admitted: tokio::sync::oneshot::Receiver<()>,
    resume: ResumeRun,
    previous_steps: Vec<FlowRunStep>,
) -> impl Future<Output = ()> + Send + 'static {
    let recovery = ActorRecoveryGuard::new(&prepared, &request, Arc::clone(&active));
    async move {
        let mut recovery = recovery;
        drive_inner(
            connection,
            request,
            prepared,
            active,
            admitted,
            Continuation::Resume {
                approvals: resume.approvals,
                rejections: resume.rejections,
                previous_steps,
            },
        )
        .await;
        recovery.disarm();
    }
}

enum Continuation {
    Start,
    Resume {
        approvals: Vec<String>,
        rejections: Vec<String>,
        previous_steps: Vec<FlowRunStep>,
    },
}

async fn drive_inner(
    connection: tinybus::Connection,
    request: StartRun,
    prepared: Prepared,
    active: Arc<ActiveRun>,
    admitted: tokio::sync::oneshot::Receiver<()>,
    continuation: Continuation,
) {
    let _ = admitted.await;
    #[cfg(test)]
    super::actor_recovery_tests::pause_after_admission_for_test(&request.run_id).await;
    let initial_seq = prepared
        .catalog
        .flow_run_event_last_seq(&request.run_id)
        .await
        .unwrap_or_default();
    let writer = EventWriter::new(
        prepared.catalog.clone(),
        request.run_id.clone(),
        initial_seq,
    );
    let previous_steps = match &continuation {
        Continuation::Start => Vec::new(),
        Continuation::Resume { previous_steps, .. } => previous_steps.clone(),
    };
    writer.emit(
        if matches!(&continuation, Continuation::Start) { "started" } else { "resumed" },
        json!({"flow_id":request.context.flow_id,"trigger_kind":request.trigger_kind,"origin":request.context.origin}),
    );
    if matches!(&continuation, Continuation::Resume { .. })
        && let Some(error) = writer.flush().await
    {
        tracing::error!(%error, run_id = %request.run_id, "resume event did not persist");
    }
    let observer = Arc::new(Observer::new(writer.clone(), previous_steps));
    let capabilities = CapsHostPorts::capabilities_with_progress(
        connection,
        request.caps_host.clone(),
        RunScope {
            flow_id: request.context.flow_id.clone(),
            storage: request.storage.clone(),
        },
        Some(observer.progress_sink()),
    )
    .await;
    let (result, timed_out) = match capabilities {
        Ok(capabilities) => {
            execute(
                &request,
                &prepared,
                &capabilities,
                &active,
                &observer,
                &continuation,
            )
            .await
        }
        Err(error) => (
            Err(anyhow::anyhow!("building run capabilities: {error}")),
            false,
        ),
    };
    let observed = observer.steps();
    let mut graph_run_id = None;
    let mut output = None;
    let mut pending_approvals = Vec::new();
    let mut failure = None;
    let mut cancelled = active.token.is_cancelled();
    match result {
        Ok((outcome, graph_id)) => {
            graph_run_id = graph_id;
            output = Some(outcome.output);
            pending_approvals = outcome.pending_approvals;
            cancelled |= outcome.cancelled;
        }
        Err(error) => {
            failure = Some(format!("{error:#}"));
        }
    }
    if let Some(error) = writer.flush().await {
        tracing::error!(%error, run_id = %request.run_id, "run observer writes did not drain cleanly");
        failure.get_or_insert(error);
    }

    let output_value = output.clone().unwrap_or(Value::Null);
    let settled_steps = run_summary::settle_steps(observed, &output_value);
    let pending = !pending_approvals.is_empty() && !cancelled && failure.is_none();
    let status = if pending {
        "pending_approval"
    } else if cancelled || timed_out {
        "cancelled"
    } else if failure.is_some() {
        "failed"
    } else {
        run_summary::terminal_status(&settled_steps, &[]).status
    };
    let error_text = if timed_out {
        Some(format!(
            "run exceeded its {} ms timeout",
            request.timeout_ms
        ))
    } else if cancelled && failure.is_none() {
        Some("run cancelled".to_owned())
    } else {
        failure
    };
    let graph_hash = pending
        .then(|| {
            tinyflows_catalog::graph_hash::compute_graph_hash(
                &prepared.compiled.graph,
                prepared.require_approval,
            )
        })
        .flatten();
    let finish = prepared
        .catalog
        .finish_flow_run(
            &request.run_id,
            status,
            &chrono::Utc::now().to_rfc3339(),
            &settled_steps,
            if pending { &pending_approvals } else { &[] },
            error_text.as_deref(),
            graph_hash.as_deref(),
        )
        .await;
    match finish {
        Ok(true) => super::record_settled_summary(&prepared.catalog, &request.run_id, status).await,
        Ok(false) => {
            tracing::warn!(run_id = %request.run_id, "run row was settled by another owner");
            unregister(&active);
            return;
        }
        Err(error) => {
            tracing::error!(%error, run_id = %request.run_id, "settling flow run row failed");
            unregister(&active);
            return;
        }
    }

    if pending {
        writer.emit(
            "parked",
            serde_json::to_value(RunParked {
                output,
                pending_approvals,
                graph_hash,
            })
            .unwrap_or(Value::Null),
        );
        if let Some(error) = writer.flush().await {
            tracing::error!(%error, run_id = %request.run_id, "parked run events did not drain cleanly");
        }
        active
            .phase
            .store(PARKED, std::sync::atomic::Ordering::Release);
        if active.token.is_cancelled() {
            if let Err(error) =
                super::cancellation::cancel_parked(&prepared, &request.run_id, &writer).await
            {
                tracing::error!(%error, run_id = %request.run_id, "settling cancellation after park");
            }
            unregister(&active);
        }
        return;
    }

    active
        .phase
        .store(super::TERMINAL, std::sync::atomic::Ordering::Release);

    let success = dedup_settle::is_success_status(status);
    settle_dedup(
        prepared.catalog.clone(),
        &request.context,
        &prepared.compiled.graph,
        success,
        &request.run_id,
    )
    .await;
    if cancelled || timed_out {
        delete_checkpoints(&prepared.checkpointer, &request.run_id, &request.run_id).await;
    }
    match writer
        .emit_terminal(super::events::finished_payload(
            status,
            error_text.clone(),
            graph_run_id.clone(),
            output,
            timed_out,
        ))
        .await
    {
        Ok(seq) => match prepared
            .catalog
            .mark_flow_run_terminal_event(&request.run_id, seq)
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                tracing::error!(run_id = %request.run_id, "terminal event cursor was not committed")
            }
            Err(error) => {
                tracing::error!(%error, run_id = %request.run_id, "committing terminal event cursor")
            }
        },
        Err(error) => {
            tracing::error!(%error, run_id = %request.run_id, "persisting terminal run event");
        }
    }
    if let Some(error) = writer.flush().await {
        tracing::error!(%error, run_id = %request.run_id, "terminal event drain failed");
    }
    unregister(&active);
}

async fn execute(
    request: &StartRun,
    prepared: &Prepared,
    capabilities: &tinyflows::caps::Capabilities,
    active: &ActiveRun,
    observer: &Arc<Observer>,
    continuation: &Continuation,
) -> (
    Result<(tinyflows::engine::RunOutcome, Option<String>)>,
    bool,
) {
    let observer_dyn: Arc<dyn tinyflows::observability::RunObserver> = observer.clone();
    let journal = observer.graph_journal();
    let checkpointer = Arc::new(prepared.checkpointer.clone());
    let run = async {
        match continuation {
            Continuation::Start => {
                let input = RunInput::new(request.trigger_data.clone())
                    .with_inputs(prepared.inputs.clone())
                    .with_run_id(request.run_id.clone())
                    .with_approvals(Vec::new());
                run_cancellable_with_checkpointer_journaled_observed(
                    &prepared.compiled,
                    input,
                    capabilities,
                    tinyflows::engine::JournaledRunOptions {
                        token: active.token.clone(),
                        checkpointer,
                        thread_id: request.run_id.clone(),
                        journal,
                    },
                    &observer_dyn,
                )
                .await
            }
            Continuation::Resume {
                approvals,
                rejections,
                ..
            } => {
                resume_cancellable_with_checkpointer_journaled_observed(
                    &prepared.compiled,
                    capabilities,
                    checkpointer,
                    &request.run_id,
                    approvals.clone(),
                    rejections.clone(),
                    journal,
                    &observer_dyn,
                    active.token.clone(),
                )
                .await
            }
        }
    };
    tokio::pin!(run);
    let timeout = Duration::from_millis(request.timeout_ms);
    let (result, timed_out) = tokio::select! {
        outcome = &mut run => (outcome, false),
        () = sleep(timeout) => {
            active.token.cancel();
            (run.await, true)
        }
    };
    let result = result
        .context(if timed_out {
            "executing timed-out workflow graph"
        } else {
            "executing workflow graph"
        })
        .map(|outcome| (outcome.outcome, Some(outcome.graph_run_ids.run_id)));
    (result, timed_out)
}

pub(super) async fn settle_dedup(
    catalog: FlowCatalogDocuments,
    context: &RunContext,
    graph: &tinyflows::model::WorkflowGraph,
    success: bool,
    run_id: &str,
) {
    let ids = graph
        .nodes
        .iter()
        .filter(|node| node.kind == tinyflows::model::NodeKind::Dedup)
        .map(|node| node.id.clone())
        .collect::<Vec<_>>();
    if ids.is_empty() {
        return;
    }
    let namespace = format!("flow:{}", context.flow_id);
    let run_id = run_id.to_owned();
    match tokio::task::spawn_blocking(move || {
        let state = FlowStateDocuments::new(catalog, namespace);
        for node in ids {
            let settled = dedup_settle::settle(&state, &node, success);
            tracing::debug!(run_id, node_id = node, ?settled, "settled dedup node state");
        }
    })
    .await
    {
        Ok(()) => {}
        Err(error) => tracing::error!(%error, "dedup settlement worker failed"),
    }
}

pub(super) async fn delete_checkpoints(
    checkpointer: &tinyflows_drivers::DriverCheckpointer<Value>,
    thread_id: &str,
    run_id: &str,
) {
    if let Err(error) = checkpointer.delete_thread(thread_id).await {
        tracing::warn!(%error, run_id, "deleting cancelled run checkpoints failed");
    }
}
