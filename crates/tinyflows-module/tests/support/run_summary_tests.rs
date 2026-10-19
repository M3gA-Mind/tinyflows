use super::*;

#[tokio::test]
#[ignore = "run through scripts/verify-module-artifact.sh"]
async fn compiled_module_updates_workflow_summaries_after_run_settlement() {
    let (_module_host, connection, catalog_proxy, runs_proxy) = module_proxies().await;
    let alice = owner(&connection, "summary-cancel", None, None, None, None).await;
    let bob = owner(&connection, "summary-complete", None, None, None, None).await;
    let alice_flow = create_flow(&catalog_proxy, &alice.storage, "Cancelled summary").await;
    let bob_flow = create_flow(&catalog_proxy, &bob.storage, "Completed summary").await;
    let alice_agent = serve_agent(&connection, "summary-cancel").await;
    let bob_agent = serve_agent(&connection, "summary-complete").await;

    let alice_run = request(
        &alice,
        alice_flow.clone(),
        "summary-cancel-run",
        graph(true),
        20_000,
    );
    runs_proxy
        .call::<runs::StartRunResponse>("StartRun", (alice_run,))
        .await
        .unwrap();
    wait_notify(&alice_agent.started, "summary cancellation agent").await;
    let cancelled: runs::CancelRunResponse = runs_proxy
        .call(
            "CancelRun",
            (RunControlRequest {
                storage: alice.storage.clone(),
                run_id: "summary-cancel-run".into(),
                cursor: 0,
            },),
        )
        .await
        .unwrap();
    assert!(cancelled.signalled);
    alice_agent.release.notify_one();
    let alice_events = poll_until_done(&runs_proxy, &alice.storage, "summary-cancel-run").await;
    assert_eq!(
        alice_events.events.last().unwrap().data["status"],
        "cancelled"
    );

    let alice_summary: Option<catalog::Workflow> = catalog_proxy
        .call("GetWorkflow", &(alice.storage.clone(), alice_flow))
        .await
        .unwrap();
    assert_eq!(
        alice_summary.unwrap().last_status.as_deref(),
        Some("cancelled"),
        "active cancellation updates the owning workflow summary"
    );

    let bob_run = request(
        &bob,
        bob_flow.clone(),
        "summary-complete-run",
        graph(true),
        20_000,
    );
    runs_proxy
        .call::<runs::StartRunResponse>("StartRun", (bob_run,))
        .await
        .unwrap();
    wait_notify(&bob_agent.started, "summary completion agent").await;
    bob_agent.release.notify_one();
    let bob_events = poll_until_done(&runs_proxy, &bob.storage, "summary-complete-run").await;
    assert_eq!(
        bob_events.events.last().unwrap().data["status"],
        "completed"
    );

    let bob_summary: Option<catalog::Workflow> = catalog_proxy
        .call("GetWorkflow", &(bob.storage.clone(), bob_flow))
        .await
        .unwrap();
    let bob_summary = bob_summary.unwrap();
    assert_eq!(bob_summary.last_status.as_deref(), Some("completed"));
    assert!(bob_summary.last_run_at.is_some());
}
