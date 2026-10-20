use super::*;
use serde_json::json;
use tinystoragedrivers_core::DocumentStoreExt;

#[tokio::test]
#[ignore = "run through scripts/verify-module-artifact.sh"]
async fn compiled_module_continues_a_legacy_parked_run_without_a_snapshot() {
    let (_module_host, connection, catalog_proxy, runs_proxy) =
        super::run_lifecycle_tests::module_proxies().await;
    let owner = owner(&connection, "legacy-resume", None, None, None, None).await;
    let graph = parked_graph();
    let flow_id = create_flow_with_graph(
        &catalog_proxy,
        &owner.storage,
        "Legacy resume",
        graph.clone(),
    )
    .await;
    let callback = serve_agent(&connection, "legacy-resume").await;
    let mut start = request(&owner, flow_id.clone(), "legacy-run", graph, 20_000);
    start.trigger_data = json!({"legacy_only": "not persisted on a pre-snapshot row"});
    runs_proxy
        .call::<runs::StartRunResponse>("StartRun", (start,))
        .await
        .unwrap();
    wait_notify(&callback.approval_started, "approval callback").await;
    poll_until_kind(&runs_proxy, &owner.storage, "legacy-run", "parked").await;

    // Model an imported pre-snapshot run: retain its real parked row and
    // checkpoint, removing only the execution snapshot written by this test's
    // new StartRun call.
    let stored = owner
        .docs
        .get("flows.runs", "legacy-run")
        .await
        .unwrap()
        .expect("the module wrote the parked run");
    assert!(stored.doc.get("execution_snapshot").is_some());
    let mut legacy = stored.doc.clone();
    legacy.as_object_mut().unwrap().remove("execution_snapshot");
    owner
        .docs
        .put(
            "flows.runs",
            "legacy-run",
            legacy,
            stored.unchanged(),
        )
        .await
        .unwrap();

    let resumed: runs::ResumeRunResponse = runs_proxy
        .call(
            "ResumeRun",
            (runs::ResumeRun {
                run_id: "legacy-run".into(),
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
        .expect("legacy checkpoint should continue through the module API");
    assert_eq!(resumed.run_id, "legacy-run");

    let terminal = poll_until_done(&runs_proxy, &owner.storage, "legacy-run").await;
    assert_eq!(terminal.events.last().unwrap().data["status"], "completed");
    let row: Option<catalog::FlowRun> = catalog_proxy
        .call(
            "GetRun",
            (RunIdRequest {
                storage: owner.storage,
                id: "legacy-run".into(),
            },),
        )
        .await
        .unwrap();
    assert_eq!(row.unwrap().status, "completed");
}
