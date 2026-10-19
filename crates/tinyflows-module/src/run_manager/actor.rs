//! Execution, persistence and settlement for a single admitted run.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use serde_json::{Value, json};
use tinyflows::{
    engine::{
        Checkpointer as _, RunInput, resume_cancellable_with_checkpointer_journaled_observed,
        run_cancellable_with_checkpointer_journaled_observed,
    },
    nodes::control_flow::dedup_settle,
};
use tinyflows_bus::runs::{ResumeRun, RunContext, RunParked, StartRun};
use tinyflows_catalog::{FlowRunStep, run_summary};
use tokio::time::sleep;

use super::events::{EventWriter, Observer};
use super::dedup_settlement::DedupSettlementScope;
use super::preparation::Prepared;
use super::{ActiveRun, PARKED, unregister};
use crate::caps::{CapsHostPorts, RunScope};

use super::actor_recovery::ActorRecoveryGuard;

pub(super) fn drive(
    connection: tinybus::Connection,
    request: StartRun,
    prepared: Prepared,
    active: Arc<ActiveRun>,
    admitted: tokio::sync::oneshot::Receiver<()>,
) -> impl Future<Output = ()> + Send + 'static {
    let recovery = ActorRecoveryGuard::new(
        &prepared.catalog,
        &prepared.checkpointer,
        &request.run_id,
        Arc::clone(&active),
    );
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
    let recovery = ActorRecoveryGuard::new(
        &prepared.catalog,
        &prepared.checkpointer,
        &request.run_id,
        Arc::clone(&active),
    );
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
    let _dedup_settlement = DedupSettlementScope::acquire(
        &request.storage,
        &request.context,
        &prepared.compiled.graph,
        &request.run_id,
    )
    .await;
    _dedup_settlement
        .settle(prepared.catalog.clone(), success)
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

pub(super) async fn delete_checkpoints(
    checkpointer: &tinyflows_drivers::DriverCheckpointer<Value>,
    thread_id: &str,
    run_id: &str,
) {
    if let Err(error) = checkpointer.delete_thread(thread_id).await {
        tracing::warn!(%error, run_id, "deleting cancelled run checkpoints failed");
    }
}
