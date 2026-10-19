use super::*;

use std::sync::{Arc, Mutex};
use tinybus::Connection;
use tinyflows::caps::{AgentInput, AgentModelSelection, AgentRunIdentity, AgentRunRequest, AgentRunner};
use tinyflows_bus::runs::{self, AgentEventsRequest, AgentEventsResponse, AgentRunHandle};
use tokio::sync::Notify;

struct CancelProbe {
    request_id: Arc<Mutex<Option<String>>>,
    events_started: Arc<Notify>,
    cancel_received: Arc<Notify>,
    cancel_id: Arc<Mutex<Option<String>>>,
}

#[tinybus::interface(name = "ai.tinyhumans.tinyflows.CapsHost")]
impl CancelProbe {
    async fn agent_run(&self, request: runs::AgentRunRequest) -> tinybus::Result<AgentRunHandle> {
        *self.request_id.lock().unwrap() = Some(request.client_request_id);
        Ok(AgentRunHandle {
            handle: "probe-run".into(),
        })
    }

    async fn agent_events(
        &self,
        _request: AgentEventsRequest,
    ) -> tinybus::Result<AgentEventsResponse> {
        self.events_started.notify_one();
        std::future::pending().await
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

#[tokio::test]
async fn dropping_agent_poll_sends_cancel_for_the_started_request_id() {
    let (module, host) = support::memory_bus().await;
    let request_id = Arc::new(Mutex::new(None));
    let cancel_id = Arc::new(Mutex::new(None));
    let probe = CancelProbe {
        request_id: request_id.clone(),
        events_started: Arc::new(Notify::new()),
        cancel_received: Arc::new(Notify::new()),
        cancel_id: cancel_id.clone(),
    };
    let events_notify = Arc::clone(&probe.events_started);
    let cancel_notify = Arc::clone(&probe.cancel_received);
    let events_started = events_notify.notified();
    let cancel_received = cancel_notify.notified();
    host.serve_at("/caps/cancel_probe".parse().unwrap(), probe)
        .await
        .unwrap();
    let runner = tokio::spawn(async move {
        let port = ports(module, "/caps/cancel_probe");
        AgentRunner::run(
            &port,
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
            },
        )
        .await
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
