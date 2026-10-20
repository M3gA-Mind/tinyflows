use super::*;
use std::sync::Mutex;
use tinyflows_bus::catalog::{CallbackAddress, CatalogStorage, ImportLegacyBatchRequest};
use tinyflows_bus::catalog::LegacyImportKind;

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
    let batches = batches.lock().unwrap();
    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0].len(), MAX_IMPORT_BATCH_RECORDS);
    assert_eq!(batches[1].len(), 1);
    assert!(batches.iter().all(|batch| batch.len() <= MAX_IMPORT_BATCH_RECORDS));

    drop(batches);
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
    let LegacyImportRecord::Invalid { record_kind, identity, .. } = &batches[0][0] else {
        panic!("oversized record should be represented as invalid")
    };
    assert_eq!(*record_kind, LegacyImportKind::Flow);
    assert_eq!(identity.as_ref().unwrap().len(), 128);
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
