use super::*;
use crate::test_support::docs;
use serde_json::json;
use tinyflows::graph::ids::NodeId;
use tinyflows_bus::catalog::{LegacyDraftRecord, LegacyImportRecord};
use tinystoragedrivers_core::DocumentStore;

fn stores() -> (FlowCatalogDocuments, DriverCheckpointer<Value>) {
    let docs = docs();
    (
        FlowCatalogDocuments::new(Arc::clone(&docs)),
        DriverCheckpointer::new(docs),
    )
}

fn checkpoint(thread_id: &str, checkpoint_id: &str) -> Checkpoint<Value> {
    Checkpoint {
        thread_id: thread_id.into(),
        checkpoint_id: checkpoint_id.into(),
        run_id: Some("run-owner".into()),
        parent_checkpoint_id: None,
        namespace: vec![],
        state: json!({"resume":"legacy"}),
        next_nodes: vec![NodeId::new("next")],
        completed_tasks: vec![],
        pending_writes: vec![],
        interrupts: vec![],
        pending_activations: None,
        barrier_arrivals: vec![],
        metadata: json!({"imported":true}),
    }
}

#[tokio::test]
async fn invalid_draft_is_reported_and_does_not_stop_later_records() {
    let (catalog, checkpointer) = stores();
    let invalid = serde_json::to_vec(&FlowDraft {
        id: "../unsafe".into(),
        flow_id: None,
        name: "unsafe".into(),
        graph: json!({}),
        origin: tinyflows_catalog::DraftOrigin::Canvas,
        created_at: "2025-01-01T00:00:00Z".into(),
        updated_at: "2025-01-01T00:00:00Z".into(),
    })
    .unwrap();
    let good = FlowDraft {
        id: "good_draft".into(),
        flow_id: None,
        name: "Good".into(),
        graph: json!({}),
        origin: tinyflows_catalog::DraftOrigin::Canvas,
        created_at: "2025-01-01T00:00:00Z".into(),
        updated_at: "2025-01-01T00:00:00Z".into(),
    };
    let records = vec![
        LegacyImportRecord::Draft(LegacyDraftRecord {
            file_name: "unsafe.json".into(),
            bytes: invalid,
        }),
        LegacyImportRecord::Draft(LegacyDraftRecord {
            file_name: "good_draft.json".into(),
            bytes: serde_json::to_vec(&good).unwrap(),
        }),
    ];
    let response = apply_legacy_batch(&records, &catalog, &checkpointer)
        .await
        .unwrap();
    assert_eq!(response.tallies[0].invalid, 1);
    assert_eq!(response.tallies[0].written, 1);
    assert_eq!(response.problems.len(), 1);
}

#[tokio::test]
async fn pending_writes_import_in_a_later_batch_and_never_replace_existing_values() {
    let (catalog, checkpointer) = stores();
    let saved = checkpoint("different-thread-than-run-id", "checkpoint-1");
    let request = [LegacyImportRecord::Checkpoint {
        raw_json: serde_json::to_string(&saved).unwrap(),
    }];
    let checkpoint_result = apply_legacy_batch(&request, &catalog, &checkpointer)
        .await
        .unwrap();
    assert_eq!(checkpoint_result.tallies[0].written, 1);

    let write = |payload| LegacyImportRecord::FileCheckpointWrite {
        thread_id: saved.thread_id.clone(),
        raw_json: serde_json::to_string(&json!({
            "namespace": [],
            "checkpoint_id": saved.checkpoint_id.clone(),
            "write": {
                "node": "next",
                "task_id": "task-1",
                "idx": 0,
                "channel": "result",
                "payload": payload
            }
        }))
        .unwrap(),
    };
    let first = apply_legacy_batch(
        &[write(json!({"value":"original"}))],
        &catalog,
        &checkpointer,
    )
    .await
    .unwrap();
    assert_eq!(first.tallies[0].written, 1);
    let replay = apply_legacy_batch(
        &[write(json!({"value":"newer-live-value"}))],
        &catalog,
        &checkpointer,
    )
    .await
    .unwrap();
    assert_eq!(replay.tallies[0].existing, 1);
    let config = CheckpointConfig {
        thread_id: saved.thread_id,
        checkpoint_id: Some(saved.checkpoint_id),
        namespace: vec![],
    };
    let writes = checkpointer.get_writes(&config).await.unwrap();
    assert_eq!(writes[0].payload, json!({"value":"original"}));
}
