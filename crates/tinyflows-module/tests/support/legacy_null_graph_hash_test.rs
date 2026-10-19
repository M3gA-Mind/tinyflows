use super::*;
use tinyflows_drivers::catalog::FlowCatalogDocuments;

#[tokio::test]
#[ignore = "run through scripts/verify-module-artifact.sh"]
async fn compiled_module_resumes_a_legacy_parked_run_without_a_graph_hash() {
    let (_module_host, connection, catalog_proxy, runs_proxy) =
        super::run_lifecycle_tests::module_proxies().await;
    let owner = owner(&connection, "legacy-null-hash", None, None, None, None).await;
    let graph = parked_graph();
    let flow_id = create_flow_with_graph(
        &catalog_proxy,
        &owner.storage,
        "Legacy approval flow",
        graph.clone(),
    )
    .await;
    let callback = serve_agent(&connection, "legacy-null-hash").await;
    let run_id = "legacy-null-hash-run";
    runs_proxy
        .call::<runs::StartRunResponse>(
            "StartRun",
            (request(&owner, flow_id.clone(), run_id, graph, 20_000),),
        )
        .await
        .unwrap();
    wait_notify(&callback.approval_started, "approval callback").await;
    poll_until_kind(&runs_proxy, &owner.storage, run_id, "parked").await;

    // Simulate the pre-graph-hash catalog row with one CAS that changes only
    // the optional hash while retaining the parked run's status and payload.
    let catalog = FlowCatalogDocuments::new(owner.docs.clone());
    let parked = catalog
        .get_flow_run(run_id)
        .await
        .unwrap()
        .expect("parked run row");
    assert!(
        parked.graph_hash.is_some(),
        "current writer pins new parked runs"
    );
    assert!(
        catalog
            .finish_parked_flow_run(
                run_id,
                "pending_approval",
                parked.finished_at.as_deref().unwrap_or(&parked.started_at),
                &parked.steps,
                &parked.pending_approvals,
                parked.error.as_deref(),
                None,
            )
            .await
            .unwrap()
    );
    assert_eq!(
        catalog
            .get_flow_run(run_id)
            .await
            .unwrap()
            .unwrap()
            .graph_hash,
        None
    );

    let resumed: runs::ResumeRunResponse = runs_proxy
        .call(
            "ResumeRun",
            (runs::ResumeRun {
                run_id: run_id.into(),
                flow_id,
                storage: owner.storage.clone(),
                caps_host: runs::CapsHost {
                    callback: owner.caps,
                },
                approvals: vec!["approval".into()],
                rejections: Vec::new(),
                timeout_ms: 20_000,
            },),
        )
        .await
        .expect("a missing legacy hash skips freshness comparison");
    assert_eq!(resumed.run_id, run_id);

    let terminal = poll_until_done(&runs_proxy, &owner.storage, run_id).await;
    assert_eq!(terminal.events.last().unwrap().data["status"], "completed");
    let outcome: runs::RunFinished =
        serde_json::from_value(terminal.events.last().unwrap().data.clone()).unwrap();
    assert!(
        outcome.output.is_some(),
        "resume returns the engine outcome"
    );
    assert!(!outcome.timed_out);
}
