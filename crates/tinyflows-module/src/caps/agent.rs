//! `AgentRunner` adapter: cursor-poll the host's per-run agent callback.

use super::*;

use tinyflows::caps::{
    AgentRunOutcome, AgentRunRequest, AgentRunner, ContextBlock, ToolDescriptor, WorkdirCheck,
};
use tinyflows::model::AgentDefinition;
use tinyflows::transcript::TranscriptEntry;
use tinyflows_bus::runs::AgentEventsRequest;

#[async_trait]
impl AgentRunner for CapsHostPorts {
    async fn run_agent(
        &self,
        agent_ref: &str,
        request: Value,
        conn: Option<&str>,
    ) -> Result<Value> {
        let request = runs::AgentRunRequest {
            agent: runs::AgentDefinition {
                id: agent_ref.to_owned(),
                ..runs::AgentDefinition::default()
            },
            model: runs::AgentModelSelection::default(),
            input: runs::AgentInput::default(),
            context: Vec::new(),
            tools: Vec::new(),
            connection_ref: conn.map(str::to_owned),
            working_dir: None,
            identity: runs::AgentRunIdentity::default(),
            metadata: Default::default(),
            output_schema: None,
            config: request,
        };
        let outcome = AgentRunner::run(self, convert(request)?).await?;
        Ok(outcome.raw)
    }

    async fn run(&self, request: AgentRunRequest) -> Result<AgentRunOutcome> {
        let node_id = request.identity.node_id.clone();
        let wire_request: runs::AgentRunRequest = convert(request)?;
        let started = tokio::time::Instant::now();
        let handle: runs::AgentRunHandle = self
            .call_one_timeout(members::AGENT_RUN, &wire_request, CALLBACK_TIMEOUT)
            .await?;
        if started.elapsed() >= CALLBACK_TIMEOUT {
            return Err(capability_error("agent callback exceeded its deadline"));
        }
        if handle.handle.is_empty() {
            return Err(capability_error("agent callback returned an empty handle"));
        }

        let mut cursor = 0;
        let mut transcript = Vec::new();
        loop {
            if started.elapsed() >= CALLBACK_TIMEOUT {
                return Err(capability_error("agent event stream exceeded its deadline"));
            }
            let remaining = CALLBACK_TIMEOUT.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                return Err(capability_error("agent event stream exceeded its deadline"));
            }
            let events: runs::AgentEventsResponse = self
                .call_one_timeout(
                    members::AGENT_EVENTS,
                    &AgentEventsRequest {
                        handle: handle.handle.clone(),
                        cursor,
                    },
                    remaining,
                )
                .await?;
            if events.events.len() > runs::MAX_AGENT_EVENTS_PAGE {
                return Err(capability_error(
                    "agent callback returned an oversized event page",
                ));
            }
            if transcript.len().saturating_add(events.events.len())
                > runs::MAX_AGENT_TRANSCRIPT_EVENTS
            {
                return Err(capability_error(
                    "agent callback exceeded the transcript event limit",
                ));
            }
            let had_events = !events.events.is_empty();
            for event in events.events {
                let Some(expected) = cursor.checked_add(1) else {
                    return Err(capability_error("agent event cursor overflowed"));
                };
                if event.seq != expected {
                    return Err(capability_error(format!(
                        "agent event sequence expected {expected}, received {}",
                        event.seq
                    )));
                }
                cursor = event.seq;
                self.publish_progress(
                    "agent_progress".into(),
                    serde_json::json!({
                        "node_id": &node_id,
                        "at_ms": event.at_ms,
                        "event_kind": &event.kind,
                        "text": &event.text,
                        "agent_seq": event.seq,
                    }),
                );
                transcript.push(TranscriptEntry::bounded(
                    event.at_ms,
                    event.kind,
                    event.text,
                ));
            }
            match (events.done, events.outcome) {
                (true, Some(outcome)) => {
                    let mut outcome: AgentRunOutcome = convert(outcome)?;
                    if outcome.transcript.len() > runs::MAX_AGENT_TRANSCRIPT_EVENTS {
                        return Err(capability_error(
                            "agent callback exceeded the transcript event limit",
                        ));
                    }
                    if outcome.transcript.is_empty() {
                        outcome.transcript = transcript;
                    } else {
                        outcome.transcript = outcome
                            .transcript
                            .into_iter()
                            .map(|entry| {
                                TranscriptEntry::bounded(entry.at_ms, entry.kind, entry.text)
                            })
                            .collect();
                    }
                    return Ok(outcome);
                }
                (false, None) if !had_events => {
                    tokio::time::sleep(EMPTY_POLL_DELAY).await;
                }
                (false, None) => {}
                (true, None) => {
                    return Err(capability_error(
                        "terminal agent event response omitted its outcome",
                    ));
                }
                (false, Some(_)) => {
                    return Err(capability_error(
                        "nonterminal agent event response included an outcome",
                    ));
                }
            }
        }
    }

    async fn resolve_agent(&self, agent_ref: &str) -> Result<Option<AgentDefinition>> {
        let agent: Option<runs::AgentDefinition> =
            self.call(members::RESOLVE_AGENT, &(agent_ref,)).await?;
        agent.map(convert).transpose()
    }

    async fn list_agents(&self) -> Result<Vec<AgentDefinition>> {
        let agents: Vec<runs::AgentDefinition> = self.call(members::LIST_AGENTS, &()).await?;
        agents.into_iter().map(convert).collect()
    }

    async fn resolve_context(
        &self,
        source: &str,
        params: &Value,
        identity: &caps::AgentRunIdentity,
    ) -> Result<Option<ContextBlock>> {
        let request = runs::ResolveContextRequest {
            source: source.to_owned(),
            params: params.clone(),
            identity: convert(identity)?,
        };
        let context: Option<runs::ContextBlock> =
            self.call_one(members::RESOLVE_CONTEXT, &request).await?;
        context.map(convert).transpose()
    }

    async fn resolve_tools(
        &self,
        grants: &[tinyflows::model::ToolGrant],
        conn: Option<&str>,
    ) -> Result<Vec<ToolDescriptor>> {
        let request = runs::ResolveToolsRequest {
            grants: convert(grants)?,
            connection_ref: conn.map(str::to_owned),
        };
        let descriptors: Vec<runs::ToolDescriptor> =
            self.call_one(members::RESOLVE_TOOLS, &request).await?;
        descriptors.into_iter().map(convert).collect()
    }

    async fn resolve_workdir(&self, workspace: &str, declared: &str) -> WorkdirCheck {
        let request = runs::ResolveWorkdirRequest {
            workspace: workspace.to_owned(),
            declared: declared.to_owned(),
        };
        let check: Result<runs::WorkdirCheck> =
            self.call_one(members::RESOLVE_WORKDIR, &request).await;
        match check {
            Ok(runs::WorkdirCheck::Unmanaged) => WorkdirCheck::Unmanaged,
            Ok(runs::WorkdirCheck::Resolved(path)) => WorkdirCheck::Resolved(path),
            Ok(runs::WorkdirCheck::Refused(reason)) => WorkdirCheck::Refused(reason),
            Err(error) => WorkdirCheck::Refused(error.to_string()),
        }
    }
}
