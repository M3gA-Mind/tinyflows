use super::*;

use std::sync::{Arc, Mutex};
use tinybus::Connection;
use tinyflows::caps::{AgentInput, AgentModelSelection, AgentRunIdentity, AgentRunRequest, AgentRunner};
use tinyflows_bus::runs::{
    self, AgentEventsRequest, AgentEventsResponse, AgentRunHandle, AgentRunOutcome, StopReason,
};
use tokio::sync::Notify;

struct CancelProbe {
    request_id: Arc<Mutex<Option<String>>>,
    events_started: Arc<Notify>,
    cancel_received: Arc<Notify>,
    cancel_id: Arc<Mutex<Option<String>>>,
    run_started: Arc<Notify>,
    run_release: Arc<Notify>,
    block_run_ack: bool,
    complete_normally: bool,
}

#[tinybus::interface(name = "ai.tinyhumans.tinyflows.CapsHost")]
impl CancelProbe {
    async fn agent_run(&self, request: runs::AgentRunRequest) -> tinybus::Result<AgentRunHandle> {
        *self.request_id.lock().unwrap() = Some(request.client_request_id);
        self.run_started.notify_one();
        if self.block_run_ack {
            self.run_release.notified().await;
        }
        Ok(AgentRunHandle {
            handle: "probe-run".into(),
        })
    }

    async fn agent_events(
        &self,
        _request: AgentEventsRequest,
    ) -> tinybus::Result<AgentEventsResponse> {
        self.events_started.notify_one();
        if self.complete_normally {
            Ok(AgentEventsResponse {
                events: vec![],
                done: true,
                outcome: Some(AgentRunOutcome {
                    stop: StopReason::Finished,
                    text: Some("done".into()),
                    json: serde_json::Value::Null,
                    raw: serde_json::json!("done"),
                    usage: None,
                    transcript: vec![],
                }),
            })
        } else {
            std::future::pending().await
        }
    }

    async fn agent_cancel(
        &self,
        request: runs::AgentCancelRequest,
    ) -> tinybus::Result<()> {
        *self.cancel_id.lock().unwrap() = Some(request.client_request_id);
        self.cancel_received.notify_one();
        Ok(())
    }
}

fn ports(connection: Connection, path: &str) -> CapsHostPorts {
    CapsHostPorts {
        connection,
        address: runs::CallbackAddress {
            destination: "test.CapsHost".into(),
            object_path: path.into(),
        },
        progress: None,
    }
}

fn probe(block_run_ack: bool, complete_normally: bool) -> CancelProbe {
    CancelProbe {
        request_id: Arc::new(Mutex::new(None)),
        events_started: Arc::new(Notify::new()),
        cancel_received: Arc::new(Notify::new()),
        cancel_id: Arc::new(Mutex::new(None)),
        run_started: Arc::new(Notify::new()),
        run_release: Arc::new(Notify::new()),
        block_run_ack,
        complete_normally,
    }
}

fn request() -> AgentRunRequest {
    AgentRunRequest {
        agent: tinyflows::model::AgentDefinition::new("agent"),
        model: AgentModelSelection::default(),
        input: AgentInput::default(),
        context: vec![],
        tools: vec![],
        connection_ref: None,
        working_dir: None,
        identity: AgentRunIdentity {
            node_id: "agent".into(),
            ..AgentRunIdentity::default()
        },
        metadata: Default::default(),
        output_schema: None,
        config: serde_json::json!({}),
    }
}

#[tokio::test]
async fn dropping_agent_poll_sends_cancel_for_the_started_request_id() {
    let (module, host) = support::memory_bus().await;
    let host_probe = probe(false, false);
    let request_id = Arc::clone(&host_probe.request_id);
    let cancel_id = Arc::clone(&host_probe.cancel_id);
    let events_notify = Arc::clone(&host_probe.events_started);
    let cancel_notify = Arc::clone(&host_probe.cancel_received);
    let events_started = events_notify.notified();
    let cancel_received = cancel_notify.notified();
    host.serve_at("/caps/cancel_probe".parse().unwrap(), host_probe)
        .await
        .unwrap();
    let runner = tokio::spawn(async move {
        let port = ports(module, "/caps/cancel_probe");
        AgentRunner::run(&port, request()).await
    });

    events_started.await;
    runner.abort();
    let _ = runner.await;
    tokio::time::timeout(std::time::Duration::from_secs(1), cancel_received)
        .await
        .expect("dropping the module future dispatches AgentCancel");
    let started = request_id.lock().unwrap().clone().unwrap();
    let cancelled = cancel_id.lock().unwrap().clone().unwrap();
    assert!(!started.is_empty());
    assert_eq!(cancelled, started);
}

#[tokio::test]
async fn dropping_agent_run_before_ack_sends_cancel_for_the_pending_request_id() {
    let (module, host) = support::memory_bus().await;
    let host_probe = probe(true, false);
    let request_id = Arc::clone(&host_probe.request_id);
    let cancel_id = Arc::clone(&host_probe.cancel_id);
    let run_notify = Arc::clone(&host_probe.run_started);
    let cancel_notify = Arc::clone(&host_probe.cancel_received);
    let run_started = run_notify.notified();
    let cancel_received = cancel_notify.notified();
    let release = Arc::clone(&host_probe.run_release);
    host.serve_at("/caps/cancel_before_ack".parse().unwrap(), host_probe)
        .await
        .unwrap();
    let runner = tokio::spawn(async move {
        let port = ports(module, "/caps/cancel_before_ack");
        AgentRunner::run(&port, request()).await
    });

    run_started.await;
    runner.abort();
    let _ = runner.await;
    tokio::time::timeout(std::time::Duration::from_secs(1), cancel_received)
        .await
        .expect("dropping the pending AgentRun call dispatches AgentCancel");
    release.notify_one();
    assert_eq!(
        cancel_id.lock().unwrap().clone(),
        request_id.lock().unwrap().clone()
    );
}

#[tokio::test]
async fn terminal_agent_outcome_disarms_cancellation_guard() {
    let (module, host) = support::memory_bus().await;
    let host_probe = probe(false, true);
    let cancel_id = Arc::clone(&host_probe.cancel_id);
    host.serve_at("/caps/cancel_terminal".parse().unwrap(), host_probe)
        .await
        .unwrap();
    let port = ports(module, "/caps/cancel_terminal");

    let outcome = AgentRunner::run(&port, request()).await.unwrap();

    assert!(outcome.is_finished());
    tokio::task::yield_now().await;
    assert_eq!(*cancel_id.lock().unwrap(), None);
}
