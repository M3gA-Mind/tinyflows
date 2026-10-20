use super::*;
use serde_json::json;
use tinyflows::graph::ids::NodeId;
use tinystoragedrivers_core::{MemoryStorage, Scope, StorageBackend};

#[tokio::test]
async fn legacy_write_import_cas_preserves_existing_task_values() {
    let storage = MemoryStorage::new();
    let docs = Arc::clone(
        storage
            .for_scope(&Scope::new("local").unwrap())
            .unwrap()
            .documents(),
    );
    let checkpointer = DriverCheckpointer::<Value>::new(docs);
    let checkpoint = Checkpoint {
        thread_id: "legacy-thread".into(),
        checkpoint_id: "legacy-checkpoint".into(),
        run_id: Some("run-owner".into()),
        parent_checkpoint_id: None,
        namespace: vec![],
        state: json!({}),
        next_nodes: vec![NodeId::new("node")],
        completed_tasks: vec![],
        pending_writes: vec![],
        interrupts: vec![],
        pending_activations: None,
        barrier_arrivals: vec![],
        metadata: json!({}),
    };
    checkpointer.put(checkpoint).await.unwrap();
    let config = CheckpointConfig {
        thread_id: "legacy-thread".into(),
        checkpoint_id: Some("legacy-checkpoint".into()),
        namespace: vec![],
    };
    checkpointer
        .put_writes(
            &config,
            &[
                PendingWrite::data("node", "data", 0, "result", json!("live")),
                PendingWrite::data("node", "control", -1, "resume", json!("live")),
            ],
        )
        .await
        .unwrap();
    let (written, existing) = checkpointer
        .import_writes_if_absent(
            &config,
            &[
                PendingWrite::data("node", "data", 0, "result", json!("legacy")),
                PendingWrite::data("node", "control", -1, "resume", json!("legacy")),
                PendingWrite::data("node", "new", 0, "result", json!("imported")),
            ],
        )
        .await
        .unwrap();
    assert_eq!((written, existing), (1, 2));
    let writes = checkpointer.get_writes(&config).await.unwrap();
    assert_eq!(writes.len(), 3);
    assert_eq!(writes[0].payload, json!("live"));
    assert_eq!(writes[1].payload, json!("live"));
    assert_eq!(writes[2].payload, json!("imported"));
}
