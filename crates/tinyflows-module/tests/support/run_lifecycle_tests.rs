use super::*;
use tinyflows_drivers::catalog::FlowCatalogDocuments;

#[tokio::test]
#[ignore = "run through scripts/verify-module-artifact.sh"]
async fn compiled_module_expires_parked_runs_and_sweeps_only_local_orphans() {
    let artifact = std::env::var_os("TINYFLOWS_MODULE_ARTIFACT")
        .expect("set TINYFLOWS_MODULE_ARTIFACT to the built cdylib");
    let broker = tinybus::broker::Broker::new();
    let module_host = ModuleHost::new(broker.clone());
    module_host
        .load_file(std::path::PathBuf::from(artifact))
        .unwrap();
    let (client, transport) = MemoryTransport::pair();
    broker.attach(Arc::new(transport));
    let connection = Connection::connect(Box::new(client)).await.unwrap();
    let catalog_proxy = connection
        .proxy(BUS_NAME, catalog::OBJECT_PATH, catalog::INTERFACE)
        .unwrap();
    let runs_proxy = connection
        .proxy(BUS_NAME, runs::OBJECT_PATH, runs::INTERFACE)
        .unwrap();
    let owner = owner(&connection, "lifecycle", None, None, None, None).await;
    let flow_id = create_flow(&catalog_proxy, &owner.storage, "Lifecycle").await;
    let documents = FlowCatalogDocuments::new(owner.docs.clone());

    documents
        .insert_flow_run("expired", &flow_id, "expired", "2020-01-01T00:00:00Z")
        .await
        .unwrap();
    documents
        .finish_flow_run(
            "expired",
            "pending_approval",
            "2020-01-01T00:01:00Z",
            &[],
            &["gate".into()],
            None,
            Some("graph-pin"),
        )
        .await
        .unwrap();
    let expiry: runs::ExpireParkedResponse = runs_proxy
        .call(
            "ExpireParked",
            (runs::ExpireParked {
                storage: owner.storage.clone(),
            },),
        )
        .await
        .unwrap();
    assert_eq!(expiry.expired, [("expired".into(), flow_id.clone())]);
    let expired: Option<catalog::FlowRun> = catalog_proxy
        .call(
            "GetRun",
            (RunIdRequest {
                storage: owner.storage.clone(),
                id: "expired".into(),
            },),
        )
        .await
        .unwrap();
    assert_eq!(expired.unwrap().status, "cancelled");
    let expired_page: runs::PollRun = runs_proxy
        .call(
            "PollRun",
            (RunControlRequest {
                storage: owner.storage.clone(),
                run_id: "expired".into(),
                cursor: 0,
            },),
        )
        .await
        .unwrap();
    assert!(expired_page.done);
    assert_eq!(
        expired_page.events.last().unwrap().data["status"],
        "cancelled"
    );

    documents
        .insert_flow_run("orphan", &flow_id, "orphan", "2020-01-01T00:00:00Z")
        .await
        .unwrap();
    let shared: runs::BootSweepResponse = runs_proxy
        .call(
            "BootSweep",
            (runs::BootSweep {
                storage: owner.storage.clone(),
                shared_backend: true,
            },),
        )
        .await
        .unwrap();
    assert!(shared.interrupted.is_empty());
    let local: runs::BootSweepResponse = runs_proxy
        .call(
            "BootSweep",
            (runs::BootSweep {
                storage: owner.storage.clone(),
                shared_backend: false,
            },),
        )
        .await
        .unwrap();
    assert_eq!(local.interrupted, ["orphan"]);
    let orphan_page: runs::PollRun = runs_proxy
        .call(
            "PollRun",
            (RunControlRequest {
                storage: owner.storage,
                run_id: "orphan".into(),
                cursor: 0,
            },),
        )
        .await
        .unwrap();
    assert!(orphan_page.done);
    assert_eq!(
        orphan_page.events.last().unwrap().data["status"],
        "interrupted"
    );
}
