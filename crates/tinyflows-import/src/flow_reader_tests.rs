use super::*;
use std::sync::Mutex;
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
        id: "too-large".into(),
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
    assert!(matches!(batches[0][0], LegacyImportRecord::Invalid { record_kind: LegacyImportKind::Flow, .. }));
}
