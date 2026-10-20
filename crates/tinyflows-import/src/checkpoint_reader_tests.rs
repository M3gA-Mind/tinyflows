use super::*;
use crate::test_support::fixtures;
use tinyflows_bus::catalog::{MAX_IMPORT_BATCH_BYTES, MAX_IMPORT_BATCH_RECORDS};
use tinystoragedrivers_sqlite::rusqlite::Connection;

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
                .filter(|record| matches!(
                    record,
                    LegacyImportRecord::SqliteCheckpointWrite(_)
                        | LegacyImportRecord::FileCheckpointWrite { .. }
                ))
                .count(),
            3
        );
        assert!(
            records
                .iter()
                .all(|record| !matches!(record, LegacyImportRecord::Invalid { .. }))
        );
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

#[tokio::test]
async fn oversized_sqlite_rows_are_counted_without_returning_their_payloads() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("checkpoints.db");
    let oversized = "x".repeat(MAX_IMPORT_RECORD_PAYLOAD_BYTES + 1);
    let connection = Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE checkpoints (seq INTEGER PRIMARY KEY, record TEXT NOT NULL);
             CREATE TABLE checkpoint_writes (
                 thread_id TEXT NOT NULL,
                 namespace TEXT NOT NULL,
                 checkpoint_id TEXT NOT NULL,
                 task_id TEXT NOT NULL,
                 idx INTEGER NOT NULL,
                 node TEXT NOT NULL,
                 channel TEXT NOT NULL,
                 payload TEXT NOT NULL
             );",
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO checkpoints (seq, record) VALUES (1, ?1), (2, ?2)",
            [oversized.as_str(), "{\"checkpoint_id\":\"small\"}"],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO checkpoint_writes VALUES
                ('thread', '[]', 'c1', 'large', 0, 'node', 'channel', ?1),
                ('thread', '[]', 'c1', 'small', 0, 'node', 'channel', '1')",
            [&oversized],
        )
        .unwrap();
    drop(connection);

    let batches = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let output = std::sync::Arc::clone(&batches);
    let mut send = move |batch| {
        let output = std::sync::Arc::clone(&output);
        async move {
            output.lock().unwrap().push(batch);
            Ok(())
        }
    };
    read_legacy_sqlite_checkpoint_batches(&path, &mut send)
        .await
        .unwrap();

    let batches = batches.lock().unwrap();
    let records: Vec<_> = batches.iter().flatten().collect();
    assert_eq!(records.len(), 4);
    assert!(matches!(
        records[0],
        LegacyImportRecord::Invalid {
            record_kind: LegacyImportKind::Checkpoint,
            ..
        }
    ));
    assert!(matches!(
        records[1],
        LegacyImportRecord::Checkpoint { raw_json } if raw_json == "{\"checkpoint_id\":\"small\"}"
    ));
    assert!(matches!(
        records[2],
        LegacyImportRecord::Invalid {
            record_kind: LegacyImportKind::Write,
            ..
        }
    ));
    assert!(matches!(
        records[3],
        LegacyImportRecord::SqliteCheckpointWrite(row) if row.task_id == "small"
    ));
    for batch in batches.iter() {
        assert!(batch.len() <= MAX_IMPORT_BATCH_RECORDS);
        assert!(serde_json::to_vec(batch).unwrap().len() <= MAX_IMPORT_BATCH_BYTES);
        assert!(batch.iter().all(|record| match record {
            LegacyImportRecord::Invalid {
                identity, message, ..
            } => {
                identity
                    .as_ref()
                    .is_none_or(|identity| identity.len() <= 128)
                    && message.len() < 256
            }
            LegacyImportRecord::Checkpoint { raw_json } => {
                raw_json.len() <= MAX_IMPORT_RECORD_PAYLOAD_BYTES
            }
            LegacyImportRecord::SqliteCheckpointWrite(row) =>
                serde_json::to_vec(row).unwrap().len() <= MAX_IMPORT_RECORD_PAYLOAD_BYTES,
            _ => false,
        }));
    }
}

#[tokio::test]
async fn jsonl_reader_splits_batches_at_record_count_and_byte_budgets() {
    let dir = tempfile::tempdir().unwrap();
    let count_dir = dir.path().join("count");
    std::fs::create_dir(&count_dir).unwrap();
    let count_lines = (0..=MAX_IMPORT_BATCH_RECORDS)
        .map(|index| format!("{{\"id\":{index}}}\n"))
        .collect::<String>();
    std::fs::write(count_dir.join("count.jsonl"), count_lines).unwrap();

    let count_batches = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let count_output = std::sync::Arc::clone(&count_batches);
    let mut count_send = move |batch| {
        let output = std::sync::Arc::clone(&count_output);
        async move {
            output.lock().unwrap().push(batch);
            Ok(())
        }
    };
    read_legacy_file_checkpoint_batches(&count_dir, &mut count_send)
        .await
        .unwrap();
    {
        let count_batches = count_batches.lock().unwrap();
        assert_eq!(count_batches.len(), 2);
        assert_eq!(count_batches[0].len(), MAX_IMPORT_BATCH_RECORDS);
        assert_eq!(count_batches[1].len(), 1);
    }

    let byte_dir = dir.path().join("bytes");
    std::fs::create_dir(&byte_dir).unwrap();
    let byte_lines = (0..12)
        .map(|index| {
            format!(
                "{{\"id\":{index},\"payload\":\"{}\"}}\n",
                "x".repeat(200_000)
            )
        })
        .collect::<String>();
    std::fs::write(byte_dir.join("bytes.jsonl"), byte_lines).unwrap();

    let byte_batches = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let byte_output = std::sync::Arc::clone(&byte_batches);
    let mut byte_send = move |batch| {
        let output = std::sync::Arc::clone(&byte_output);
        async move {
            output.lock().unwrap().push(batch);
            Ok(())
        }
    };
    read_legacy_file_checkpoint_batches(&byte_dir, &mut byte_send)
        .await
        .unwrap();
    let byte_batches = byte_batches.lock().unwrap();
    assert!(byte_batches.len() > 1);
    assert_eq!(byte_batches.iter().map(Vec::len).sum::<usize>(), 12);
    for batch in byte_batches.iter() {
        assert!(batch.len() <= MAX_IMPORT_BATCH_RECORDS);
        assert!(serde_json::to_vec(batch).unwrap().len() <= MAX_IMPORT_BATCH_BYTES);
        assert!(batch.iter().all(|record| matches!(
            record,
            LegacyImportRecord::Checkpoint { raw_json }
                if raw_json.len() <= MAX_IMPORT_RECORD_PAYLOAD_BYTES
        )));
    }
}
