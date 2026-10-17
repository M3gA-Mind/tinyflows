//! Adapters from the exact per-run `CapsHost` callback to engine capabilities.

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use tinybus::Connection;
use tinyflows::{
    caps::{
        self, AgentRunOutcome, AgentRunRequest, AgentRunner, ApprovalDecision, ApprovalOutcome,
        ApprovalProvider, ApprovalRequest, CodeLanguage, CodeRunner, ContextBlock, HttpClient,
        LlmProvider, MemoryProvider, ShellInterpreter, ShellOutcome, ShellRequest, ShellRunner,
        ShellScript, StateStore, TaskRunner, TokioTaskRunner, ToolDescriptor, ToolInvoker,
        WorkdirCheck, WorkflowResolver,
    },
    error::{EngineError, Result},
};
use tinyflows::{model::AgentDefinition, transcript::TranscriptEntry};
use tinyflows_bus::{
    catalog::CatalogStorage,
    runs::{self, AgentEventsRequest, CapsHost, caps as members},
};
use tinyflows_drivers::catalog::{FlowCatalogDocuments, FlowStateDocuments};
use tinystoragedrivers_core::DocumentStore;

#[path = "caps/task_executor.rs"]
mod task_executor;
use task_executor::{CapabilityTaskExecutor, TaskRunnerLink};

/// Maximum time a host operation or agent event stream may take.
const CALLBACK_TIMEOUT: Duration = Duration::from_secs(2 * 60 * 60);
/// Delay between empty, nonterminal agent event polls.
const EMPTY_POLL_DELAY: Duration = Duration::from_millis(25);

/// Engine adapters fixed to one host-created callback address and run context.
pub struct CapsHostPorts {
    connection: Connection,
    address: runs::CallbackAddress,
}

/// Trusted values supplied by run management, separate from graph and trigger
/// JSON. `storage` is already bound to the acting owner scope.
pub struct RunScope {
    /// The trusted flow id used to namespace durable workflow state.
    pub flow_id: String,
    /// Document callback already bound by the host to the owner scope.
    pub storage: CatalogStorage,
}

impl CapsHostPorts {
    /// Builds one run's engine bundle. The host callback address must already
    /// be bound to that run's flow, run id, approval policy and owner scope.
    /// Builds engine capabilities for one run. The supplied callback and
    /// storage handles must be the ones created for this run by the trusted
    /// host; graph and trigger data must never fill these fields.
    pub async fn capabilities(
        connection: Connection,
        host: CapsHost,
        scope: RunScope,
    ) -> Result<caps::Capabilities> {
        let documents =
            super::catalog_documents::Documents::new(connection.clone(), scope.storage.callback)
                .await
                .map_err(capability_error)?;
        let catalog = FlowCatalogDocuments::new(Arc::new(documents) as Arc<dyn DocumentStore>);
        let state: Arc<dyn StateStore> = Arc::new(FlowStateDocuments::new(
            catalog.clone(),
            format!("flow:{}", scope.flow_id),
        ));
        let resolver: Arc<dyn WorkflowResolver> = Arc::new(CatalogResolver { catalog });
        let ports = Arc::new(Self {
            connection,
            address: host.callback,
        });
        let mut capabilities = caps::Capabilities {
            llm: ports.clone(),
            tools: ports.clone(),
            http: ports.clone(),
            code: ports.clone(),
            state,
            resolver,
            agent: Some(ports.clone()),
            shell: Some(ports.clone()),
            memory: Some(ports.clone()),
            tasks: None,
            approvals: Some(ports),
        };
        // The executor sees the same host capabilities but no task runner, so
        // child workflows can still execute their spawn specs inline without
        // creating a runner -> capabilities -> runner reference cycle.
        let task_runner = Arc::new(TaskRunnerLink::default());
        let executor = Arc::new(CapabilityTaskExecutor::new(
            capabilities.clone(),
            Arc::clone(&task_runner),
        ));
        let runner = Arc::new(TokioTaskRunner::with_executor(executor)) as Arc<dyn TaskRunner>;
        task_runner.bind(&runner);
        capabilities.tasks = Some(runner);
        Ok(capabilities)
    }

    async fn call<A, R>(&self, member: &str, args: &A) -> Result<R>
    where
        A: Serialize + Sync,
        R: DeserializeOwned,
    {
        let proxy = self
            .connection
            .proxy(
                &self.address.destination,
                &self.address.object_path,
                runs::CAPS_INTERFACE,
            )
            .map_err(capability_error)?
            .with_timeout(CALLBACK_TIMEOUT);
        proxy.call(member, args).await.map_err(capability_error)
    }

    async fn call_one<A, R>(&self, member: &str, request: &A) -> Result<R>
    where
        A: Serialize + Sync,
        R: DeserializeOwned,
    {
        self.call(member, &(request,)).await
    }

    async fn call_one_timeout<A, R>(
        &self,
        member: &str,
        request: &A,
        timeout: Duration,
    ) -> Result<R>
    where
        A: Serialize + Sync,
        R: DeserializeOwned,
    {
        let proxy = self
            .connection
            .proxy(
                &self.address.destination,
                &self.address.object_path,
                runs::CAPS_INTERFACE,
            )
            .map_err(capability_error)?
            .with_timeout(timeout);
        proxy
            .call(member, &(request,))
            .await
            .map_err(capability_error)
    }
}

struct CatalogResolver {
    catalog: FlowCatalogDocuments,
}

#[async_trait]
impl WorkflowResolver for CatalogResolver {
    async fn resolve(&self, id: &str) -> Result<tinyflows::model::WorkflowGraph> {
        let graph = self
            .catalog
            .get_flow(id)
            .await
            .map_err(capability_error)?
            .map(|flow| flow.graph)
            .ok_or_else(|| capability_error(format!("missing workflow {id}")))?;
        tinyflows::compat::ensure_compatible(&graph).map_err(capability_error)?;
        Ok(graph)
    }
}

fn capability_error(error: impl std::fmt::Display) -> EngineError {
    EngineError::Capability(error.to_string())
}

fn convert<A: Serialize, B: DeserializeOwned>(value: A) -> Result<B> {
    let wire = serde_json::to_value(value).map_err(capability_error)?;
    serde_json::from_value(wire).map_err(capability_error)
}

#[cfg(test)]
#[path = "caps_tests.rs"]
mod tests;

#[async_trait]
impl LlmProvider for CapsHostPorts {
    async fn complete(&self, request: Value, conn: Option<&str>) -> Result<Value> {
        self.call(members::LLM_COMPLETE, &(request, conn)).await
    }
}

#[async_trait]
impl ToolInvoker for CapsHostPorts {
    async fn invoke(&self, slug: &str, args: Value, conn: Option<&str>) -> Result<Value> {
        self.call(members::TOOL_INVOKE, &(slug, args, conn)).await
    }
}

#[async_trait]
impl HttpClient for CapsHostPorts {
    async fn request(&self, request: Value, conn: Option<&str>) -> Result<Value> {
        self.call(members::HTTP_REQUEST, &(request, conn)).await
    }
}

#[async_trait]
impl CodeRunner for CapsHostPorts {
    async fn run(&self, language: CodeLanguage, source: &str, input: Value) -> Result<Value> {
        let language = match language {
            CodeLanguage::JavaScript => runs::CodeLanguage::JavaScript,
            CodeLanguage::Python => runs::CodeLanguage::Python,
        };
        let request = runs::CodeRunRequest {
            language,
            source: source.to_owned(),
            input,
        };
        self.call_one(members::CODE_RUN, &request).await
    }
}

#[async_trait]
impl ShellRunner for CapsHostPorts {
    async fn run(&self, request: ShellRequest) -> Result<ShellOutcome> {
        let request = runs::ShellRunRequest {
            interpreter: match request.interpreter {
                ShellInterpreter::Sh => runs::ShellInterpreter::Sh,
                ShellInterpreter::Bash => runs::ShellInterpreter::Bash,
            },
            script: match request.script {
                ShellScript::Inline(script) => runs::ShellScript::Inline(script),
                ShellScript::Path(path) => runs::ShellScript::Path(path),
            },
            cwd: request.cwd,
            env: request.env,
            input: request.input,
        };
        let outcome: runs::ShellRunOutcome = self.call_one(members::SHELL_RUN, &request).await?;
        Ok(ShellOutcome {
            exit_code: outcome.exit_code,
            stdout: outcome.stdout,
            stderr: outcome.stderr,
        })
    }
}

#[async_trait]
impl MemoryProvider for CapsHostPorts {
    async fn recall(&self, scope: &str, query: &str, opts: Value) -> Result<Value> {
        self.call(members::MEMORY_RECALL, &(scope, query, opts))
            .await
    }

    async fn flavour(&self, slug: &str) -> Result<Value> {
        self.call(members::MEMORY_FLAVOUR, &(slug,)).await
    }

    async fn people(&self, query: Option<&str>) -> Result<Value> {
        self.call(members::MEMORY_PEOPLE, &(query,)).await
    }

    async fn remember(&self, scope: &str, key: &str, value: Value) -> Result<()> {
        self.call(members::MEMORY_REMEMBER, &(scope, key, value))
            .await
    }

    async fn forget(&self, scope: &str, key: &str) -> Result<()> {
        self.call(members::MEMORY_FORGET, &(scope, key)).await
    }
}

#[async_trait]
impl ApprovalProvider for CapsHostPorts {
    async fn decide(&self, request: &ApprovalRequest) -> Result<ApprovalOutcome> {
        let wire = runs::ApprovalRequest {
            request_id: request.request_id.clone(),
            node_id: request.node_id.clone(),
            run_id: request.run_id.clone(),
            title: request.title.clone(),
            prompt: request.prompt.clone(),
            subject: runs::ApprovalSubject {
                kind: request.subject.kind.clone(),
                value: request.subject.value.clone(),
            },
            assignees: request.assignees.clone(),
            metadata: request.metadata.clone(),
        };
        match self
            .call_one::<_, runs::ApprovalOutcome>(members::APPROVAL_DECIDE, &wire)
            .await?
        {
            runs::ApprovalOutcome::Pending => Ok(ApprovalOutcome::Pending),
            runs::ApprovalOutcome::Decided(decision) => {
                Ok(ApprovalOutcome::Decided(ApprovalDecision {
                    approved: decision.approved,
                    decided_by: decision.decided_by,
                    comment: decision.comment,
                    payload: decision.payload,
                }))
            }
        }
    }

    async fn cancel(&self, request_id: &str, reason: &str) -> Result<()> {
        self.call_one(
            members::APPROVAL_CANCEL,
            &runs::ApprovalCancelRequest {
                request_id: request_id.to_owned(),
                reason: reason.to_owned(),
            },
        )
        .await
    }
}

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
        let wire_request: runs::AgentRunRequest = convert(request)?;
        let handle: runs::AgentRunHandle = self
            .call_one_timeout(members::AGENT_RUN, &wire_request, CALLBACK_TIMEOUT)
            .await?;
        if handle.handle.is_empty() {
            return Err(capability_error("agent callback returned an empty handle"));
        }

        let started = tokio::time::Instant::now();
        let mut cursor = 0;
        let mut transcript = Vec::new();
        loop {
            if started.elapsed() >= CALLBACK_TIMEOUT {
                return Err(capability_error("agent event stream exceeded its deadline"));
            }
            let events: runs::AgentEventsResponse = self
                .call_one_timeout(
                    members::AGENT_EVENTS,
                    &AgentEventsRequest {
                        handle: handle.handle.clone(),
                        cursor,
                    },
                    CALLBACK_TIMEOUT.saturating_sub(started.elapsed()),
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
                transcript.push(TranscriptEntry::bounded(
                    event.at_ms,
                    event.kind,
                    event.text,
                ));
            }
            match (events.done, events.outcome) {
                (true, Some(outcome)) => {
                    let mut outcome: AgentRunOutcome = convert(outcome)?;
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
