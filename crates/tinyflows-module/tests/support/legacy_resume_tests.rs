use super::*;
use serde_json::json;
use tinyflows::engine::Checkpointer as _;
use tinyflows_drivers::DriverCheckpointer;

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
    assert_eq!(
        terminal.events.last().unwrap().data["status"],
        "completed",
        "terminal event: {:?}",
        terminal.events.last()
    );
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

#[tokio::test]
#[ignore = "run through scripts/verify-module-artifact.sh"]
async fn compiled_module_resumes_an_imported_legacy_row_by_its_checkpoint_thread_id() {
    let (_module_host, connection, catalog_proxy, runs_proxy) =
        super::run_lifecycle_tests::module_proxies().await;
    let source = owner(&connection, "legacy-source", None, None, None, None).await;
    let target = owner(&connection, "legacy-target", None, None, None, None).await;
    let graph = parked_graph();
    let source_flow = create_flow_with_graph(
        &catalog_proxy,
        &source.storage,
        "Legacy checkpoint source",
        graph.clone(),
    )
    .await;
    let target_flow = create_flow_with_graph(
        &catalog_proxy,
        &target.storage,
        "Imported legacy checkpoint",
        graph.clone(),
    )
    .await;
    let source_callback = serve_agent(&connection, "legacy-source").await;
    let _target_callback = serve_agent(&connection, "legacy-target").await;
    let checkpoint_thread = "legacy-checkpoint-thread";
    let start = request(&source, source_flow, checkpoint_thread, graph, 20_000);
    runs_proxy
        .call::<runs::StartRunResponse>("StartRun", (start,))
        .await
        .unwrap();
    wait_notify(&source_callback.approval_started, "source approval callback").await;
    poll_until_kind(
        &runs_proxy,
        &source.storage,
        checkpoint_thread,
        "parked",
    )
    .await;
    let source_row: Option<catalog::FlowRun> = catalog_proxy
        .call(
            "GetRun",
            (RunIdRequest {
                storage: source.storage.clone(),
                id: checkpoint_thread.into(),
            },),
        )
        .await
        .unwrap();
    let source_row = source_row.unwrap();
    let source_checkpointer = DriverCheckpointer::<serde_json::Value>::new(source.docs.clone());
    let checkpoints = source_checkpointer
        .get_thread(checkpoint_thread)
        .await
        .unwrap();
    assert!(!checkpoints.is_empty(), "fixture must carry a real checkpoint");

    let mut records = vec![catalog::LegacyImportRecord::Run(
        catalog::LegacyRunRecord {
            id: "legacy-imported-run".into(),
            flow_id: target_flow.clone(),
            thread_id: checkpoint_thread.into(),
            status: "pending_approval".into(),
            started_at: source_row.started_at,
            finished_at: source_row.finished_at,
            steps_json: serde_json::to_string(&source_row.steps).unwrap(),
            pending_approvals_json:
                serde_json::to_string(&source_row.pending_approvals).unwrap(),
            error: None,
            graph_hash: source_row.graph_hash,
        },
    )];
    records.extend(checkpoints.into_iter().map(|checkpoint| {
        catalog::LegacyImportRecord::Checkpoint {
            raw_json: serde_json::to_string(&checkpoint).unwrap(),
        }
    }));
    let imported: catalog::ImportLegacyBatchResponse = catalog_proxy
        .call(
            "ImportLegacyBatch",
            &(catalog::ImportLegacyBatchRequest {
                storage: target.storage.clone(),
                records,
            },),
        )
        .await
        .unwrap();
    assert!(imported.problems.is_empty(), "{imported:?}");

    let raw = target
        .docs
        .get("flows.runs", "legacy-imported-run")
        .await
        .unwrap()
        .unwrap();
    assert!(raw.doc.get("execution_snapshot").is_none());
    assert!(raw.doc.get("events").is_none());
    assert!(raw.doc.get("terminal_event_seq").is_none());
    assert_ne!("legacy-imported-run", checkpoint_thread);

    runs_proxy
        .call::<runs::ResumeRunResponse>(
            "ResumeRun",
            (runs::ResumeRun {
                run_id: "legacy-imported-run".into(),
                flow_id: target_flow,
                storage: target.storage.clone(),
                caps_host: runs::CapsHost {
                    callback: target.caps.clone(),
                },
                approvals: vec!["approval".into()],
                rejections: Vec::new(),
                timeout_ms: 20_000,
            },),
        )
        .await
        .unwrap();
    let terminal = poll_until_done(&runs_proxy, &target.storage, "legacy-imported-run").await;
    assert_eq!(terminal.events.last().unwrap().data["status"], "completed");
    let resumed = terminal
        .events
        .iter()
        .find(|event| event.kind == "resumed")
        .unwrap();
    assert_eq!(resumed.data["legacy_resume"], true);
    assert!(resumed.data.get("trigger_kind").is_none());
    assert!(resumed.data.get("origin").is_none());
    assert!(target_checkpointer_has_no_thread(&target, checkpoint_thread).await);
}

async fn target_checkpointer_has_no_thread(owner: &run_artifact_support::Owner, thread: &str) -> bool {
    DriverCheckpointer::<serde_json::Value>::new(owner.docs.clone())
        .get_thread(thread)
        .await
        .unwrap()
        .is_empty()
}
