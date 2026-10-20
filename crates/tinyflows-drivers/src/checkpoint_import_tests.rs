use super::*;
use serde_json::json;
use std::sync::Arc;
use tinyflows::graph::ids::NodeId;
use tinystoragedrivers_core::{MemoryStorage, Scope, StorageBackend};
#[cfg(feature = "catalog")]
use tinystoragedrivers_core::{Precondition, WriteOptions};

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

#[tokio::test]
async fn duplicate_legacy_control_writes_keep_the_last_value() {
    let storage = MemoryStorage::new();
    let docs = Arc::clone(
        storage
            .for_scope(&Scope::new("local").unwrap())
            .unwrap()
            .documents(),
    );
    let checkpointer = DriverCheckpointer::<Value>::new(docs);
    checkpointer
        .put(Checkpoint {
            thread_id: "legacy-control-retry".into(),
            checkpoint_id: "legacy-checkpoint-control-retry".into(),
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
        })
        .await
        .unwrap();
    let config = CheckpointConfig {
        thread_id: "legacy-control-retry".into(),
        checkpoint_id: Some("legacy-checkpoint-control-retry".into()),
        namespace: vec![],
    };

    let result = checkpointer
        .import_writes_if_absent(
            &config,
            &[
                PendingWrite::data("node", "retry", -1, "resume", json!("first")),
                PendingWrite::data("node", "retry", -1, "resume", json!("latest")),
            ],
        )
        .await
        .unwrap();

    assert_eq!(result, (2, 0));
    let writes = checkpointer.get_writes(&config).await.unwrap();
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].payload, json!("latest"));
}

#[cfg(feature = "catalog")]
#[tokio::test]
async fn legacy_write_import_retries_after_concurrent_live_write_and_counts_the_retry_state() {
    let storage = MemoryStorage::new();
    let docs = Arc::clone(
        storage
            .for_scope(&Scope::new("local").unwrap())
            .unwrap()
            .documents(),
    );
    let initial = DriverCheckpointer::<Value>::new(Arc::clone(&docs));
    initial
        .put(Checkpoint {
            thread_id: "legacy-thread-race".into(),
            checkpoint_id: "legacy-checkpoint-race".into(),
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
        })
        .await
        .unwrap();
    let config = CheckpointConfig {
        thread_id: "legacy-thread-race".into(),
        checkpoint_id: Some("legacy-checkpoint-race".into()),
        namespace: vec![],
    };
    initial
        .put_writes(
            &config,
            &[
                PendingWrite::data("node", "data", 0, "result", json!("live")),
                PendingWrite::data("node", "control", -1, "resume", json!("live")),
            ],
        )
        .await
        .unwrap();

    let writes_id = crate::checkpoint_layout::writes_id_key(
        crate::checkpoint_layout::NAMESPACE,
        &config.thread_id,
        &crate::checkpoint_layout::subgraph_key(&config.namespace),
        config.checkpoint_id.as_deref().unwrap(),
    );
    let interjection: crate::catalog::test_support::Interjection = Box::new(move |inner| {
        Box::pin(async move {
            let current = inner
                .get("graph.writes", &writes_id)
                .await
                .unwrap()
                .unwrap();
            let mut document = current.doc;
            document["writes"].as_array_mut().unwrap().push(json!({
                "node":"node","task_id":"new","idx":0,
                "channel":"result","payload":"concurrent-live"
            }));
            document["writes"].as_array_mut().unwrap().push(json!({
                "node":"node","task_id":"import-control","idx":-1,
                "channel":"resume","payload":"concurrent-control"
            }));
            inner
                .put_with(
                    "graph.writes",
                    &writes_id,
                    document,
                    Precondition::Version(current.version),
                    WriteOptions::new(),
                )
                .await
                .unwrap();
        })
    });
    let wrapped = crate::catalog::test_support::Interposed::wrap(
        docs,
        ("put_with", "graph.writes"),
        interjection,
    );
    let importer = DriverCheckpointer::<Value>::new(wrapped);
    let (written, existing) = importer
        .import_writes_if_absent(
            &config,
            &[
                PendingWrite::data("node", "data", 0, "result", json!("legacy")),
                PendingWrite::data("node", "control", -1, "resume", json!("legacy")),
                PendingWrite::data("node", "new", 0, "result", json!("legacy")),
                PendingWrite::data("node", "import-control", -1, "resume", json!("first")),
                PendingWrite::data("node", "import-control", -1, "resume", json!("latest")),
            ],
        )
        .await
        .unwrap();
    assert_eq!((written, existing), (0, 5));
    let writes = importer.get_writes(&config).await.unwrap();
    assert_eq!(writes.len(), 4);
    assert_eq!(writes[2].payload, json!("concurrent-live"));
    assert_eq!(writes[3].payload, json!("concurrent-control"));
}
