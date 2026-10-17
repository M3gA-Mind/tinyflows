use super::*;
use serde_json::json;
use tinytools::Tool;

#[tokio::test]
async fn propose_workflow_is_propose_only_and_uses_non_revision_payload() {
    let host = Arc::new(crate::tools::fakes::MemHost::default());
    let tool = ProposeWorkflowTool::new(
        host.clone(),
        Arc::new(super::super::ports::PortableNodeKinds),
    );
    assert_eq!(tool.name(), "propose_workflow");
    assert_eq!(tool.permission_level(), PermissionLevel::None);
    assert!(!tool.external_effect());
    let result = tool
        .execute(json!({
            "name":"Example",
            "graph":{"nodes":[{"id":"start","name":"Start","kind":"trigger"}],"edges":[]}
        }))
        .await
        .unwrap();
    assert!(!result.is_error);
    let proposal: Value = serde_json::from_str(&result.text()).unwrap();
    assert_eq!(proposal["type"], "workflow_proposal");
    assert_eq!(proposal["persisted"], false);
    assert_eq!(proposal["revision"], false);
    assert!(
        tool.description()
            .contains("Node kinds and their required config:")
    );
}
