//! Compile and exercise the checkpoint importer APIs provided by `catalog`.
#![cfg(feature = "catalog")]

use std::sync::Arc;

use serde_json::Value;
use tinyflows_drivers::DriverCheckpointer;
use tinyflows_import::{import_file_checkpoints, import_sqlite_checkpoints};
use tinystoragedrivers_core::{MemoryStorage, Scope, StorageBackend};

#[tokio::test]
async fn catalog_feature_retains_checkpoint_import_entry_points() {
    let directory = tempfile::tempdir().unwrap();
    let docs = Arc::clone(
        MemoryStorage::new()
            .for_scope(&Scope::new("local").unwrap())
            .unwrap()
            .documents(),
    );
    let checkpointer = DriverCheckpointer::<Value>::new(docs);

    let sqlite = import_sqlite_checkpoints(
        &directory.path().join("missing-checkpoints.db"),
        &checkpointer,
    )
    .await
    .unwrap();
    let files = import_file_checkpoints(
        &directory.path().join("missing-file-checkpoints"),
        &checkpointer,
    )
    .await
    .unwrap();

    assert!(sqlite.is_clean());
    assert!(files.is_clean());
}
