use super::tests::{connections, graph, put_proxy, scope};
use serde_json::{Value, json};
use tinyflows_bus::catalog::{self, LegacyFlowRecord, LegacyImportRecord};

#[tokio::test]
async fn legacy_import_batches_preserve_historic_enablement_and_are_idempotent() {
    let (_server, host) = connections().await;
    let storage = scope(&host, "legacy_import").await;
    let proxy = put_proxy(&host).unwrap();
    let request = catalog::ImportLegacyBatchRequest {
        storage: storage.clone(),
        records: vec![LegacyImportRecord::Flow(LegacyFlowRecord {
            id: "historic-scheduled-flow".into(),
            name: "Historic scheduled flow".into(),
            graph_json: serde_json::to_string(&graph("schedule", false)).unwrap(),
            enabled: true,
            created_at: "2025-01-02T03:04:05Z".into(),
            updated_at: "2025-01-03T03:04:05Z".into(),
            last_run_at: None,
            last_status: None,
            require_approval: false,
        })],
    };

    let first: catalog::ImportLegacyBatchResponse = proxy
        .call(catalog::members::IMPORT_LEGACY_BATCH, &(request.clone(),))
        .await
        .expect("Catalog serves legacy batch import");
    assert_eq!(first.tallies[0].kind, catalog::LegacyImportKind::Flow);
    assert_eq!(first.tallies[0].written, 1);
    assert_eq!(first.tallies[0].existing, 0);
    assert!(first.problems.is_empty());

    let imported: Option<catalog::Workflow> = proxy
        .call("GetWorkflow", &(storage.clone(), "historic-scheduled-flow"))
        .await
        .unwrap();
    let imported = imported.expect("legacy flow is present");
    assert!(imported.enabled, "import must retain historic enablement");
    assert!(
        !imported.require_approval,
        "import must retain historic approval state"
    );

    let second: catalog::ImportLegacyBatchResponse = proxy
        .call(catalog::members::IMPORT_LEGACY_BATCH, &(request,))
        .await
        .expect("replaying the batch is supported");
    assert_eq!(second.tallies[0].written, 0);
    assert_eq!(second.tallies[0].existing, 1);
}

#[tokio::test]
async fn legacy_import_rejects_oversized_batches_before_writing() {
    let (_server, host) = connections().await;
    let storage = scope(&host, "legacy_import_limits").await;
    let proxy = put_proxy(&host).unwrap();
    let records = (0..=catalog::MAX_IMPORT_BATCH_RECORDS)
        .map(|index| {
            LegacyImportRecord::Invalid {
                record_kind: catalog::LegacyImportKind::Flow,
                identity: Some(index.to_string()),
                message: "test record".into(),
            }
        })
        .collect();
    let reply: Result<Value, _> = proxy
        .call(
            catalog::members::IMPORT_LEGACY_BATCH,
            &(catalog::ImportLegacyBatchRequest { storage, records },),
        )
        .await;
    assert!(reply.is_err(), "oversized batches must be rejected");
}
