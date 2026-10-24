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
    let cancelled: runs::RunFinished =
        serde_json::from_value(alice_events.events.last().unwrap().data.clone()).unwrap();
    assert!(
        cancelled.output.unwrap().is_object(),
        "the module retains partial engine output after explicit cancellation"
    );
    assert!(!cancelled.timed_out);

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
    assert_eq!(
        bob_events.events.last().unwrap().data["output"],
        json!({
            "nodes": {
                "agent": {
                    "_activation_step": 2,
                    "items": [{"json": {
                        "json": {},
                        "meta": {"agent_ref":"test-agent","stop":"finished"},
                        "raw": {},
                        "text":"done"
                    }}],
                    "port": null
                },
                "output": {
                    "_activation_step": 3,
                    "items": [{
                        "json": {
                            "declared":"declared-value",
                            "json": {},
                            "meta": {"agent_ref":"test-agent","stop":"finished"},
                            "payload":"trigger-value",
                            "raw": {},
                            "text":"done"
                        },
                        "paired_item": 0
                    }],
                    "port": null
                },
                "trigger": {
                    "items": [{"json":{"payload":"trigger-value"}}],
                    "port": null
                }
            },
            "run": {
                "approvals": [],
                "id":"summary-complete-run",
                "inputs":{"declared":"declared-value"},
                "trigger":{"payload":"trigger-value"}
            }
        }),
        "PollRun retains the exact engine outcome for the legacy run response"
    );

    let bob_summary: Option<catalog::Workflow> = catalog_proxy
        .call("GetWorkflow", &(bob.storage.clone(), bob_flow))
        .await
        .unwrap();
    let bob_summary = bob_summary.unwrap();
    assert_eq!(bob_summary.last_status.as_deref(), Some("completed"));
    assert!(bob_summary.last_run_at.is_some());

    let parked_owner = owner(&connection, "summary-park", None, None, None, None).await;
    let parked_graph = parked_graph();
    let parked_flow = create_flow_with_graph(
        &catalog_proxy,
        &parked_owner.storage,
        "Parked summary",
        parked_graph.clone(),
    )
    .await;
    let parked_agent = serve_agent(&connection, "summary-park").await;
    let mut parked_run = request(
        &parked_owner,
        parked_flow,
        "summary-park-run",
        parked_graph,
        20_000,
    );
    parked_run.trigger_data = json!({"id":"summary-park-item","payload":"review-value"});
    runs_proxy
        .call::<runs::StartRunResponse>("StartRun", (parked_run,))
        .await
        .unwrap();
    wait_notify(&parked_agent.approval_started, "summary approval callback").await;
    let parked_events = poll_until_kind(
        &runs_proxy,
        &parked_owner.storage,
        "summary-park-run",
        "parked",
    )
    .await;
    let parked = parked_events
        .last()
        .expect("parked event is available through PollRun");
    assert!(parked.data.get("output").is_some());
    assert_eq!(parked.data["pending_approvals"], json!(["approval"]));
}
