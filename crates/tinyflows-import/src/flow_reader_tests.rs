use super::*;
use std::sync::Mutex;
use std::io::Cursor;
use tinyflows_bus::catalog::LegacyImportKind;
use tinyflows_bus::catalog::{
    CallbackAddress, CatalogStorage, ImportLegacyBatchRequest, MAX_IMPORT_BATCH_BYTES,
    MAX_IMPORT_RECORD_PAYLOAD_BYTES, MAX_IMPORT_REQUEST_ENVELOPE_BYTES,
};
use tinystoragedrivers_sqlite::rusqlite::Connection;

#[tokio::test]
async fn batches_are_bounded_and_a_single_oversized_record_becomes_invalid() {
    let mut sender = BatchSender::default();
    let batches = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&batches);
    let mut send = move |batch| {
        let captured = Arc::clone(&captured);
        async move {
            captured.lock().unwrap().push(batch);
            Ok(())
        }
    };
    for index in 0..=MAX_IMPORT_BATCH_RECORDS {
        sender
            .push(
                LegacyImportRecord::Invalid {
                    record_kind: LegacyImportKind::Flow,
                    identity: Some(index.to_string()),
                    message: "source problem".into(),
                },
                &mut send,
            )
            .await
            .unwrap();
    }
    sender.flush(&mut send).await.unwrap();
    {
        let batches = batches.lock().unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].len(), MAX_IMPORT_BATCH_RECORDS);
        assert_eq!(batches[1].len(), 1);
        assert!(
            batches
                .iter()
                .all(|batch| batch.len() <= MAX_IMPORT_BATCH_RECORDS)
        );
    }
    let mut sender = BatchSender::default();
    let oversized = LegacyImportRecord::Flow(LegacyFlowRecord {
        id: "i".repeat(10_000),
        name: "legacy".into(),
        graph_json: "x".repeat(MAX_IMPORT_BATCH_BYTES + 1),
        enabled: true,
        created_at: "2025-01-01T00:00:00Z".into(),
        updated_at: "2025-01-01T00:00:00Z".into(),
        last_run_at: None,
        last_status: None,
        require_approval: false,
    });
    let captured = Arc::new(Mutex::new(Vec::new()));
    let out = Arc::clone(&captured);
    let mut send = move |batch| {
        let out = Arc::clone(&out);
        async move {
            out.lock().unwrap().push(batch);
            Ok(())
        }
    };
    sender.push(oversized, &mut send).await.unwrap();
    sender.flush(&mut send).await.unwrap();
    let batches = captured.lock().unwrap();
    let LegacyImportRecord::Invalid {
        record_kind,
        identity,
        ..
    } = &batches[0][0]
    else {
        panic!("oversized record should be represented as invalid")
    };
    assert_eq!(*record_kind, LegacyImportKind::Flow);
    assert_eq!(identity.as_ref().unwrap().len(), 128);
}

#[tokio::test]
async fn escape_expansion_is_bounded_and_later_records_still_import() {
    let mut sender = BatchSender::default();
    let escaped = LegacyImportRecord::Flow(LegacyFlowRecord {
        id: "escaped".into(),
        name: "legacy".into(),
        graph_json: "\u{1}".repeat(MAX_IMPORT_RECORD_PAYLOAD_BYTES / 4),
        enabled: true,
        created_at: "2025-01-01T00:00:00Z".into(),
        updated_at: "2025-01-01T00:00:00Z".into(),
        last_run_at: None,
        last_status: None,
        require_approval: false,
    });
    let valid = LegacyImportRecord::Flow(LegacyFlowRecord {
        id: "later".into(),
        name: "valid".into(),
        graph_json: "{}".into(),
        enabled: true,
        created_at: "2025-01-02T00:00:00Z".into(),
        updated_at: "2025-01-02T00:00:00Z".into(),
        last_run_at: None,
        last_status: None,
        require_approval: false,
    });
    let batches = Arc::new(Mutex::new(Vec::new()));
    let output = Arc::clone(&batches);
    let mut send = move |batch| {
        let output = Arc::clone(&output);
        async move {
            output.lock().unwrap().push(batch);
            Ok(())
        }
    };
    sender.push(escaped, &mut send).await.unwrap();
    sender.push(valid, &mut send).await.unwrap();
    sender.flush(&mut send).await.unwrap();

    let batches = batches.lock().unwrap();
    assert!(matches!(
        batches[0][0],
        LegacyImportRecord::Invalid {
            record_kind: LegacyImportKind::Flow,
            ..
        }
    ));
    assert!(matches!(
        batches[0][1],
        LegacyImportRecord::Flow(ref row) if row.id == "later"
    ));
}

#[test]
fn sqlite_octet_length_counts_text_bytes() {
    let connection = Connection::open_in_memory().unwrap();
    let bytes = connection
        .query_row("SELECT octet_length('é')", [], |row| row.get::<_, i64>(0))
        .unwrap();
    assert_eq!(bytes, 2);
}

#[tokio::test]
async fn oversized_catalog_rows_and_drafts_are_invalid_but_later_records_survive() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("flows.db");
    let oversized_graph = "x".repeat(MAX_IMPORT_RECORD_PAYLOAD_BYTES + 1);
    let connection = Connection::open(&db_path).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE flow_definitions (
                id TEXT, name TEXT, graph_json TEXT, enabled INTEGER,
                created_at TEXT, updated_at TEXT, last_run_at TEXT,
                last_status TEXT, require_approval INTEGER
            );",
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO flow_definitions VALUES
                ('oversized', 'large graph', ?1, 1, '2025-01-01', '2025-01-01', NULL, NULL, 0),
                ('later-flow', 'valid graph', '{}', 1, '2025-01-02', '2025-01-02', NULL, NULL, 0)",
            [&oversized_graph],
        )
        .unwrap();
    drop(connection);

    let drafts = dir.path().join("drafts");
    std::fs::create_dir(&drafts).unwrap();
    std::fs::write(
        drafts.join("a-oversized.json"),
        vec![b'x'; MAX_IMPORT_RECORD_PAYLOAD_BYTES + 1],
    )
    .unwrap();
    std::fs::write(drafts.join("z-valid.json"), br#"{"draft":true}"#).unwrap();

    let output = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&output);
    read_legacy_flow_catalog(dir.path(), move |batch| {
        let captured = Arc::clone(&captured);
        async move {
            captured.lock().unwrap().extend(batch);
            Ok(())
        }
    })
    .await
    .unwrap();

    let output = output.lock().unwrap();
    assert!(output.iter().any(|record| matches!(
        record,
        LegacyImportRecord::Invalid {
            record_kind: LegacyImportKind::Flow,
            ..
        }
    )));
    assert!(output.iter().any(|record| matches!(
        record,
        LegacyImportRecord::Flow(row) if row.id == "later-flow"
    )));
    assert!(output.iter().any(|record| matches!(
        record,
        LegacyImportRecord::Invalid {
            record_kind: LegacyImportKind::Draft,
            identity: Some(name),
            ..
        } if name == "a-oversized.json"
    )));
    assert!(output.iter().any(|record| matches!(
        record,
        LegacyImportRecord::Draft(row) if row.file_name == "z-valid.json"
    )));
}

#[test]
fn oversized_sqlite_projection_avoids_materializing_the_source_value() {
    let connection = Connection::open_in_memory().unwrap();
    connection
        .execute_batch("CREATE TABLE source (id TEXT, body TEXT);")
        .unwrap();
    connection
        .execute(
            "INSERT INTO source VALUES ('large', ?1)",
            ["x".repeat(256 * 1024)],
        )
        .unwrap();
    unsafe {
        tinystoragedrivers_sqlite::rusqlite::ffi::sqlite3_limit(
            connection.handle(),
            tinystoragedrivers_sqlite::rusqlite::ffi::SQLITE_LIMIT_LENGTH,
            4096,
        );
    }

    let query = bounded_page_query(
        "source",
        &fields(&["id", "body"]),
        &["id", "body"],
        "id",
    );
    let row = connection
        .query_row(&query, [1024_i64, 0_i64], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, bool>(2)?,
            ))
        })
        .unwrap();
    assert_eq!(row, ("large".into(), None, true));
}

#[test]
fn draft_read_consumes_at_most_the_limit_plus_one_bytes() {
    let input = Cursor::new(vec![b'x'; 1024 * 1024]);
    let mut input = input;
    let bytes = read_bounded(&mut input, 17).unwrap();
    assert_eq!(bytes.len(), 18);
    assert_eq!(input.position(), 18);
}

#[tokio::test]
async fn reader_payload_reserves_envelope_space_for_the_actual_wire_request() {
    let mut sender = BatchSender::default();
    let address = CallbackAddress {
        destination: "d".repeat(1_800),
        object_path: format!("/{}", "p".repeat(1_800)),
    };
    let storage = CatalogStorage { callback: address };
    let envelope_size = serde_json::to_vec(&storage).unwrap().len();
    assert!(envelope_size < MAX_IMPORT_REQUEST_ENVELOPE_BYTES);

    let record = LegacyImportRecord::Flow(LegacyFlowRecord {
        id: "near-limit".into(),
        name: "legacy".into(),
        graph_json: "g".repeat(MAX_IMPORT_RECORD_PAYLOAD_BYTES - 600),
        enabled: true,
        created_at: "2025-01-01T00:00:00Z".into(),
        updated_at: "2025-01-01T00:00:00Z".into(),
        last_run_at: None,
        last_status: None,
        require_approval: false,
    });
    let captured = Arc::new(Mutex::new(Vec::new()));
    let output = Arc::clone(&captured);
    let mut send = move |batch| {
        let output = Arc::clone(&output);
        async move {
            output.lock().unwrap().push(batch);
            Ok(())
        }
    };
    sender.push(record, &mut send).await.unwrap();
    sender.flush(&mut send).await.unwrap();
    let records = captured.lock().unwrap().remove(0);
    assert!(matches!(&records[0], LegacyImportRecord::Flow(_)));
    let request = ImportLegacyBatchRequest { storage, records };
    let encoded = serde_json::to_vec(&request).unwrap();
    assert!(encoded.len() <= MAX_IMPORT_BATCH_BYTES);
    assert!(encoded.len() >= MAX_IMPORT_BATCH_BYTES - 1_000);
}
