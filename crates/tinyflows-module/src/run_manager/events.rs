//! Non-blocking observer and durable event writer for one flow run.

use std::sync::{Arc, Mutex};

use anyhow::Result;
use serde_json::{Value, json};
use tokio::sync::{mpsc, oneshot};
use tinyflows::observability::{ExecutionStep, Run, RunObserver, RunStatus, StepStatus};
use tinyflows_bus::runs::RunEvent;
use tinyflows_catalog::FlowRunStep;
use tinyflows_drivers::catalog::FlowCatalogDocuments;
use crate::caps::RunProgressSink;

async fn persist_event(
    catalog: &FlowCatalogDocuments,
    run_id: &str,
    seq: &mut u64,
    kind: String,
    data: Value,
    graph_run_id: Option<String>,
    last_error: &mut Option<String>,
) -> Result<()> {
    let event = RunEvent {
        seq: seq.saturating_add(1),
        kind,
        data,
        graph_run_id,
    };
    match catalog
        .append_flow_run_event(run_id, serde_json::to_value(&event)?)
        .await
    {
        Ok(()) => {
            *seq = event.seq;
            Ok(())
        }
        Err(error) => {
            let message = error.to_string();
            *last_error = Some(message.clone());
            Err(anyhow::anyhow!(message))
        }
    }
}

enum WriteCommand {
    Step {
        step: FlowRunStep,
        data: Value,
    },
    Event {
        kind: String,
        data: Value,
        graph_run_id: Option<String>,
        ack: Option<oneshot::Sender<Result<()>>>,
    },
    Barrier(oneshot::Sender<Option<String>>),
}

#[derive(Clone)]
pub(super) struct EventWriter {
    sender: mpsc::UnboundedSender<WriteCommand>,
}

impl EventWriter {
    pub fn new(catalog: FlowCatalogDocuments, run_id: String) -> Self {
        let (sender, mut receiver) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            let mut seq = 0_u64;
            let mut last_error = None;
            while let Some(command) = receiver.recv().await {
                match command {
                    WriteCommand::Step { step, data } => {
                        if let Err(error) = catalog.upsert_flow_run_step(&run_id, &step).await {
                            last_error = Some(format!("persisting run step: {error}"));
                        }
                        persist_event(
                            &catalog,
                            &run_id,
                            &mut seq,
                            "step".into(),
                            data,
                            None,
                            &mut last_error,
                        )
                        .await;
                    }
                    WriteCommand::Event {
                        kind,
                        data,
                        graph_run_id,
                        ack,
                    } => {
                        let ack_result = persist_event(
                            &catalog,
                            &run_id,
                            &mut seq,
                            kind,
                            data,
                            graph_run_id,
                            &mut last_error,
                        )
                        .await;
                        if let Some(ack) = ack {
                            let _ = ack.send(ack_result);
                        }
                    }
                    WriteCommand::Barrier(ack) => {
                        let _ = ack.send(last_error.take());
                    }
                }
            }
        });
        Self { sender }
    }

    pub fn emit(&self, kind: impl Into<String>, data: Value) {
        let _ = self.sender.send(WriteCommand::Event {
            kind: kind.into(),
            data,
            graph_run_id: None,
            ack: None,
        });
    }

    pub fn graph_observation(&self, observation: &tinyflows::engine::GraphObservation) {
        let _ = self.sender.send(WriteCommand::Event {
            kind: "graph_observation".into(),
            data: serde_json::to_value(observation).unwrap_or(Value::Null),
            graph_run_id: Some(observation.run_id.as_str().to_owned()),
            ack: None,
        });
    }

    pub async fn emit_terminal(&self, kind: &str, data: Value) -> Result<()> {
        let (ack, result) = oneshot::channel();
        self.sender
            .send(WriteCommand::Event {
                kind: kind.to_owned(),
                data,
                graph_run_id: None,
                ack: Some(ack),
            })
            .map_err(|_| anyhow::anyhow!("run event writer stopped"))?;
        result.await.map_err(|_| anyhow::anyhow!("run event writer stopped"))??;
        Ok(())
    }

    pub async fn flush(&self) -> Option<String> {
        let (ack, result) = oneshot::channel();
        if self.sender.send(WriteCommand::Barrier(ack)).is_err() {
            return Some("run event writer stopped".into());
        }
        result.await.unwrap_or_else(|_| Some("run event writer stopped".into()))
    }

    fn step(&self, step: FlowRunStep) {
        let data = serde_json::to_value(&step).unwrap_or(Value::Null);
        let _ = self.sender.send(WriteCommand::Step { step, data });
    }
}

pub(super) struct Observer {
    writer: EventWriter,
    steps: Arc<Mutex<Vec<FlowRunStep>>>,
}

impl Observer {
    pub fn new(writer: EventWriter) -> Self {
        Self {
            writer,
            steps: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn steps(&self) -> Vec<FlowRunStep> {
        self.steps
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub fn progress_sink(&self) -> RunProgressSink {
        let writer = self.writer.clone();
        Arc::new(move |kind, data| writer.emit(kind, data))
    }
}

impl RunObserver for Observer {
    fn on_run_start(&self, run_id: &str) {
        self.writer
            .emit("engine_started", json!({"engine_run_id": run_id}));
    }

    fn on_step_start(&self, node_id: &str) {
        self.writer.emit("step_started", json!({"node_id": node_id}));
    }

    fn on_step_finish(&self, step: &ExecutionStep) {
        let status = match &step.status {
            StepStatus::Success => "success",
            StepStatus::Error => "error",
        };
        let step = FlowRunStep {
            node_id: step.node_id.clone(),
            output: step.output.clone(),
            port: None,
            status: Some(status.into()),
            duration_ms: Some(u64::try_from(step.duration_ms).unwrap_or(u64::MAX)),
            diagnostics: step
                .diagnostics
                .iter()
                .map(|diagnostic| serde_json::to_value(diagnostic).unwrap_or(Value::Null))
                .collect(),
        };
        self.steps
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(step.clone());
        self.writer.step(step);
    }

    fn on_run_finish(&self, run: &Run) {
        let status = match &run.status {
            RunStatus::Running => "running",
            RunStatus::Completed => "completed",
            RunStatus::CompletedWithErrors => "completed_with_errors",
            RunStatus::Failed => "failed",
        };
        self.writer
            .emit("engine_settled", json!({"engine_run_id": run.id,"status":status}));
    }
}
