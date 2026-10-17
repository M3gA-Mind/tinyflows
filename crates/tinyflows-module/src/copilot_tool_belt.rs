use std::sync::Arc;

use serde_json::Value;
use tinybus::{Connection, Error};
use tinyflows_copilot::tools::ports::{
    AgentDirectory, BuilderGates, DraftStore, FlowStore, NodeKindSource, SuggestionStore,
};

use super::copilot_tools::ScopedCopilotHost;

pub(super) struct ScopedNodeKinds(Vec<tinyflows::catalog::NodeKindContract>);

impl ScopedNodeKinds {
    pub fn from_wire(contracts: Vec<Value>) -> Result<Self, Error> {
        let contracts = if contracts.is_empty() {
            tinyflows::catalog::all_contracts()
        } else {
            contracts
                .into_iter()
                .map(serde_json::from_value)
                .collect::<Result<Vec<_>, _>>()
                .map_err(Error::failed)?
        };
        Ok(Self(contracts))
    }
}

impl NodeKindSource for ScopedNodeKinds {
    fn all(&self) -> Vec<tinyflows::catalog::NodeKindContract> {
        self.0.clone()
    }

    fn get(&self, kind: &str) -> Option<tinyflows::catalog::NodeKindContract> {
        self.0
            .iter()
            .find(|contract| contract.kind == kind)
            .cloned()
    }
}

pub(super) async fn invoke_tool(
    connection: Connection,
    request: tinyflows_bus::copilot::InvokeToolRequest,
) -> Result<Value, Error> {
    let host = Arc::new(ScopedCopilotHost {
        connection,
        host: request.host,
    });
    let kinds = Arc::new(ScopedNodeKinds::from_wire(request.node_kinds)?);
    let tool = tool_belt(host, kinds)
        .into_iter()
        .find(|tool| tool.name() == request.name)
        .ok_or_else(|| Error::failed(format!("unknown copilot tool '{}'", request.name)))?;
    // The callback address came from the trusted host request envelope. Tool
    // arguments remain model data and cannot select or override that address.
    let result = tool
        .execute(request.arguments)
        .await
        .map_err(|error| Error::failed(error.to_string()))?;
    serde_json::to_value(result).map_err(Error::failed)
}

pub(super) fn tool_belt(
    host: Arc<ScopedCopilotHost>,
    kinds: Arc<ScopedNodeKinds>,
) -> Vec<Box<dyn tinytools::Tool>> {
    use tinyflows_copilot::tools::*;

    let drafts: Arc<dyn DraftStore> = host.clone();
    let flows: Arc<dyn FlowStore> = host.clone();
    let gates: Arc<dyn BuilderGates> = host.clone();
    let agents: Arc<dyn AgentDirectory> = host.clone();
    let suggestions: Arc<dyn SuggestionStore> = host.clone();
    vec![
        Box::new(ProposeWorkflowTool::new(gates.clone(), kinds.clone())),
        Box::new(ReviseWorkflowTool::new(gates.clone())),
        Box::new(EditWorkflowTool::new(
            drafts.clone(),
            flows.clone(),
            gates.clone(),
        )),
        Box::new(ValidateWorkflowTool::new(
            drafts.clone(),
            flows.clone(),
            gates.clone(),
        )),
        Box::new(GetFlowHistoryTool::new(flows.clone())),
        Box::new(ListFlowRunsTool::new(flows.clone())),
        Box::new(ResumeFlowRunTool::new(flows.clone())),
        Box::new(CancelFlowRunTool::new(flows.clone())),
        Box::new(CreateWorkflowTool::new(flows.clone(), gates.clone())),
        Box::new(DuplicateFlowTool::new(flows.clone())),
        Box::new(ListFlowsTool::new(flows.clone())),
        Box::new(GetFlowTool::new(flows.clone())),
        Box::new(GetFlowRunTool::new(flows.clone())),
        Box::new(ListAgentDefinitionsTool::new(agents)),
        Box::new(ListNodeKindsTool::new(kinds.clone())),
        Box::new(GetNodeKindContractTool::new(kinds)),
        Box::new(DryRunWorkflowTool::new(
            drafts.clone(),
            flows.clone(),
            gates.clone(),
        )),
        Box::new(RunFlowTool::new(flows.clone())),
        Box::new(SaveWorkflowTool::new(drafts, flows, gates)),
        Box::new(SuggestWorkflowsTool::new(suggestions)),
    ]
}
