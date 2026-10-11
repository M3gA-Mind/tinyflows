//! Mechanical host callback projection; all portable gate policy stays in the engine.

use serde_json::Value;
use tinyflows::{
    caps::WorkflowResolver,
    error::{EngineError, Result},
    gates::{BuilderGateHost, HostGateStage},
    model::WorkflowGraph,
};
use tinyflows_bus::{AuthoringHost, HOST_INTERFACE, host};

pub(super) struct HostPorts {
    pub connection: tinybus::Connection,
    pub host: AuthoringHost,
}

impl HostPorts {
    fn proxy(&self) -> Result<tinybus::Proxy> {
        self.connection
            .proxy(&self.host.bus_name, &self.host.object_path, HOST_INTERFACE)
            .map_err(|error| EngineError::Capability(error.to_string()))
    }
}

#[async_trait::async_trait]
impl WorkflowResolver for HostPorts {
    async fn resolve(&self, id: &str) -> Result<WorkflowGraph> {
        let graph: Option<Value> = self
            .proxy()?
            .call(host::RESOLVE_WORKFLOW, (id,))
            .await
            .map_err(|error| EngineError::Capability(error.to_string()))?;
        let graph =
            graph.ok_or_else(|| EngineError::Capability(format!("missing workflow {id}")))?;
        tinyflows::migrate::deserialize_graph(graph).map_err(EngineError::Capability)
    }
}

#[async_trait::async_trait]
impl BuilderGateHost for HostPorts {
    async fn check(&self, stage: HostGateStage, graph: &WorkflowGraph) -> Result<Vec<String>> {
        let stage = match stage {
            HostGateStage::ToolPolicy => host::TOOL_POLICY,
            HostGateStage::AgentReferences => host::AGENT_REFERENCES,
            HostGateStage::Connections => host::CONNECTIONS,
            HostGateStage::ToolContracts => host::TOOL_CONTRACTS,
        };
        let graph = serde_json::to_value(graph)
            .map_err(|error| EngineError::Capability(error.to_string()))?;
        self.proxy()?
            .call(host::CHECK_GATE, (stage, graph))
            .await
            .map_err(|error| EngineError::Capability(error.to_string()))
    }
}
