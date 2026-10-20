use super::*;
use crate::test_support::fixtures;

#[tokio::test]
async fn sqlite_and_jsonl_checkpoint_sources_emit_raw_ordered_rows() {
    for (sqlite, files) in [
        (
            fixtures().join("checkpoints/checkpoints.db"),
            fixtures().join("checkpoints/absent"),
        ),
        (
            fixtures().join("checkpoints/absent.db"),
            fixtures().join("checkpoints/file"),
        ),
    ] {
        let batches = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let out = std::sync::Arc::clone(&batches);
        let mut send = move |batch| {
            let out = std::sync::Arc::clone(&out);
            async move {
                out.lock().unwrap().push(batch);
                Ok(())
            }
        };
        read_legacy_checkpoint_batches(&sqlite, &files, &mut send)
            .await
            .unwrap();
        let batches = batches.lock().unwrap();
        let records: Vec<_> = batches.iter().flatten().collect();
        assert_eq!(
            records
                .iter()
                .filter(|record| matches!(record, LegacyImportRecord::Checkpoint { .. }))
                .count(),
            4
        );
        assert_eq!(
            records
                .iter()
                .filter(|record| matches!(record, LegacyImportRecord::SqliteCheckpointWrite(_) | LegacyImportRecord::FileCheckpointWrite { .. }))
                .count(),
            3
        );
        assert!(records.iter().all(|record| !matches!(record, LegacyImportRecord::Invalid { .. })));
    }
}

#[tokio::test]
async fn oversized_jsonl_line_becomes_a_bounded_invalid_record() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("thread.jsonl");
    std::fs::write(&path, vec![b'x'; MAX_IMPORT_RECORD_PAYLOAD_BYTES + 1]).unwrap();
    let batches = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let out = std::sync::Arc::clone(&batches);
    let mut send = move |batch| {
        let out = std::sync::Arc::clone(&out);
        async move {
            out.lock().unwrap().push(batch);
            Ok(())
        }
    };
    read_legacy_checkpoint_batches(&dir.path().join("missing.db"), dir.path(), &mut send)
        .await
        .unwrap();
    let batches = batches.lock().unwrap();
    assert!(matches!(
        &batches[0][0],
        LegacyImportRecord::Invalid {
            record_kind: LegacyImportKind::Checkpoint,
            ..
        }
    ));
}
