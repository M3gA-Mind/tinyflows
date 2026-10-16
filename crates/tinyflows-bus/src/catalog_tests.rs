use super::*;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::json;

fn wire_round_trip<T>(value: T)
where
    T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug,
{
    let bytes = serde_json::to_vec(&value).unwrap();
    let decoded: T = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(decoded, value);
}

fn storage() -> CatalogStorage {
    CatalogStorage {
        callback: CallbackAddress {
            destination: "test.CatalogStorage".into(),
            object_path: "/test/agent/alice/CatalogStorage".into(),
        },
    }
}

fn workflow() -> Workflow {
    Workflow {
        id: "flow-1".into(),
        name: "Example".into(),
        enabled: false,
        graph: json!({"nodes": [], "edges": []}),
        created_at: "2026-01-01T00:00:00Z".into(),
        updated_at: "2026-01-02T00:00:00Z".into(),
        last_run_at: None,
        last_status: Some("completed".into()),
        require_approval: true,
    }
}

#[test]
fn every_catalog_contract_type_round_trips_on_the_wire() {
    let workflow = workflow();
    let storage = storage();
    wire_round_trip(DocCapabilitiesRequest {});
    wire_round_trip(DocCapabilities { transactions: true });
    wire_round_trip(DocEnsureRequest {
        spec: json!({"name":"flows_definitions"}),
    });
    wire_round_trip(DocGetRequest {
        collection: "flows_definitions".into(),
        id: "flow-1".into(),
    });
    wire_round_trip(DocPutRequest {
        collection: "flows_definitions".into(),
        id: "flow-1".into(),
        doc: json!({"name":"Example"}),
        precondition: json!({"kind":"absent"}),
    });
    wire_round_trip(DocDeleteRequest {
        collection: "flows_definitions".into(),
        id: "flow-1".into(),
        precondition: json!({"kind":"version","version":1}),
    });
    wire_round_trip(DocQueryRequest {
        collection: "flows_definitions".into(),
        query: json!({"filter":{"kind":"all"}}),
    });
    wire_round_trip(DocCountRequest {
        collection: "flows_revisions".into(),
        filter: json!({"flow_id":"flow-1"}),
    });
    wire_round_trip(DocDeleteWhereRequest {
        collection: "flows_runs".into(),
        filter: json!({"flow_id":"flow-1"}),
    });
    wire_round_trip(DocAtomicBatchRequest {
        ops: vec![json!({"op":"put"})],
    });
    wire_round_trip(CatalogStorageFailure {
        kind: "conflict".into(),
        message: "version changed".into(),
    });
    wire_round_trip(CatalogStorageReply::Value {
        value: json!({"id":"flow-1"}),
    });
    wire_round_trip(CatalogStorageReply::Error {
        error: CatalogStorageFailure {
            kind: "not_found".into(),
            message: "missing".into(),
        },
    });
    wire_round_trip(storage.callback.clone());
    wire_round_trip(storage.clone());
    wire_round_trip(workflow.clone());
    wire_round_trip(WorkflowSummary {
        id: workflow.id.clone(),
        name: workflow.name.clone(),
        revision: 3,
        enabled: workflow.enabled,
        require_approval: workflow.require_approval,
        last_status: workflow.last_status.clone(),
        trigger_kind: Some("schedule".into()),
        node_count: 4,
        updated_at: workflow.updated_at.clone(),
    });
    wire_round_trip(WorkflowRevision {
        id: "revision-1".into(),
        flow_id: workflow.id.clone(),
        graph: workflow.graph.clone(),
        name: workflow.name.clone(),
        require_approval: workflow.require_approval,
        created_at: workflow.created_at.clone(),
    });
    wire_round_trip(PutWorkflowRequest::Create {
        storage: storage.clone(),
        name: "New".into(),
        graph: json!({"nodes": [], "edges": []}),
        require_approval: false,
    });
    wire_round_trip(PutWorkflowRequest::Update {
        storage: storage.clone(),
        id: workflow.id.clone(),
        name: workflow.name.clone(),
        graph: workflow.graph.clone(),
        require_approval: true,
        expected_version: Some(workflow.updated_at.clone()),
        force_disarm_automatic: true,
    });
    wire_round_trip(PutWorkflowRequest::Duplicate {
        storage: storage.clone(),
        id: workflow.id.clone(),
        name: "Example (copy)".into(),
    });
    wire_round_trip(PutWorkflowResult {
        workflow: workflow.clone(),
        born_disabled: true,
        approval_forced: true,
    });
    wire_round_trip(FlowUpdateError::VersionConflict {
        message: "changed".into(),
        current: workflow.clone(),
    });
    wire_round_trip(FlowUpdateError::NotFound {
        message: "missing".into(),
    });
    wire_round_trip(FlowUpdateError::Store {
        message: "unavailable".into(),
    });
    wire_round_trip(PutWorkflowResponse::Ok(PutWorkflowResult {
        workflow: workflow.clone(),
        born_disabled: false,
        approval_forced: false,
    }));
    wire_round_trip(PutWorkflowResponse::Error(
        FlowUpdateError::VersionConflict {
            message: "changed".into(),
            current: workflow.clone(),
        },
    ));
    wire_round_trip(ListWorkflowsResult {
        workflows: vec![],
        skipped_rows: 2,
    });
    wire_round_trip(DeleteWorkflowResult {
        id: workflow.id.clone(),
        removed: true,
    });
    wire_round_trip(SetEnabledRequest {
        storage: storage.clone(),
        id: workflow.id.clone(),
        enabled: true,
    });
    wire_round_trip(ListRevisionsRequest {
        storage: storage.clone(),
        flow_id: workflow.id.clone(),
        limit: 20,
    });
    wire_round_trip(GetRevisionRequest {
        storage,
        flow_id: workflow.id,
        revision_id: "revision-1".into(),
    });
}

#[test]
fn served_catalog_version_and_member_list_match_the_new_surface() {
    assert_eq!(CONTRACT_VERSION, 2);
    assert_eq!(
        METHODS,
        [
            "ListWorkflows",
            "GetWorkflow",
            "PutWorkflow",
            "DeleteWorkflow",
            "SetEnabled",
            "ListRevisions",
            "GetRevision",
        ]
    );
}
