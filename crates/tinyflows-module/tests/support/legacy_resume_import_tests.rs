use super::*;
use serde_json::json;
use tinyflows::engine::Checkpointer as _;
use tinyflows_drivers::DriverCheckpointer;

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
    let mut start = request(&source, source_flow, checkpoint_thread, graph, 20_000);
    start.trigger_data = json!({"id":"imported-legacy-item"});
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

    let resume = runs::ResumeRun {
        run_id: "legacy-imported-run".into(),
        flow_id: target_flow,
        storage: target.storage.clone(),
        caps_host: runs::CapsHost {
            callback: target.caps.clone(),
        },
        approvals: vec!["approval".into()],
        rejections: Vec::new(),
        timeout_ms: 20_000,
    };
    let (first, second) = tokio::join!(
        runs_proxy.call::<runs::ResumeRunResponse>("ResumeRun", (resume.clone(),)),
        runs_proxy.call::<runs::ResumeRunResponse>("ResumeRun", (resume,)),
    );
    assert_ne!(first.is_ok(), second.is_ok(), "exactly one resume may claim the imported row");
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
    assert!(target_checkpointer_has_thread(&target, checkpoint_thread).await);
    assert!(target_checkpointer_has_no_thread(&target, "legacy-imported-run").await);
}

#[tokio::test]
#[ignore = "run through scripts/verify-module-artifact.sh"]
async fn imported_legacy_resume_does_not_borrow_a_checkpoint_from_another_owner() {
    let (_module_host, connection, catalog_proxy, runs_proxy) =
        super::run_lifecycle_tests::module_proxies().await;
    let source = owner(&connection, "foreign-checkpoint-source", None, None, None, None).await;
    let target = owner(&connection, "foreign-checkpoint-target", None, None, None, None).await;
    let graph = parked_graph();
    let source_flow = create_flow_with_graph(
        &catalog_proxy,
        &source.storage,
        "Foreign checkpoint source",
        graph.clone(),
    )
    .await;
    let target_flow = create_flow_with_graph(
        &catalog_proxy,
        &target.storage,
        "Foreign checkpoint target",
        graph.clone(),
    )
    .await;
    let callback = serve_agent(&connection, "foreign-checkpoint-source").await;
    let thread_id = "foreign-owner-checkpoint";
    let mut start = request(&source, source_flow, thread_id, graph, 20_000);
    start.trigger_data = json!({"id":"foreign-item"});
    runs_proxy
        .call::<runs::StartRunResponse>("StartRun", (start,))
        .await
        .unwrap();
    wait_notify(&callback.approval_started, "source approval callback").await;
    poll_until_kind(&runs_proxy, &source.storage, thread_id, "parked").await;
    let source_row: Option<catalog::FlowRun> = catalog_proxy
        .call(
            "GetRun",
            (RunIdRequest {
                storage: source.storage.clone(),
                id: thread_id.into(),
            },),
        )
        .await
        .unwrap();
    let source_row = source_row.unwrap();
    assert!(target_checkpointer_has_thread(&source, thread_id).await);

    let imported: catalog::ImportLegacyBatchResponse = catalog_proxy
        .call(
            "ImportLegacyBatch",
            &(catalog::ImportLegacyBatchRequest {
                storage: target.storage.clone(),
                records: vec![catalog::LegacyImportRecord::Run(
                    catalog::LegacyRunRecord {
                        id: "legacy-foreign-run".into(),
                        flow_id: target_flow.clone(),
                        thread_id: thread_id.into(),
                        status: "pending_approval".into(),
                        started_at: source_row.started_at,
                        finished_at: source_row.finished_at,
                        steps_json: serde_json::to_string(&source_row.steps).unwrap(),
                        pending_approvals_json:
                            serde_json::to_string(&source_row.pending_approvals).unwrap(),
                        error: None,
                        graph_hash: None,
                    },
                )],
            },),
        )
        .await
        .unwrap();
    assert!(imported.problems.is_empty(), "{imported:?}");
    assert!(!target_checkpointer_has_thread(&target, thread_id).await);

    let error = runs_proxy
        .call::<runs::ResumeRunResponse>(
            "ResumeRun",
            (runs::ResumeRun {
                run_id: "legacy-foreign-run".into(),
                flow_id: target_flow,
                storage: target.storage.clone(),
                caps_host: runs::CapsHost {
                    callback: target.caps,
                },
                approvals: vec!["approval".into()],
                rejections: Vec::new(),
                timeout_ms: 20_000,
            },),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("legacy parked run has no durable checkpoint"));
    let failed: Option<catalog::FlowRun> = catalog_proxy
        .call(
            "GetRun",
            (RunIdRequest {
                storage: target.storage.clone(),
                id: "legacy-foreign-run".into(),
            },),
        )
        .await
        .unwrap();
    assert_eq!(failed.unwrap().status, "failed");
    assert!(
        target_checkpointer_has_thread(&source, thread_id).await,
        "a checkpoint owned by another scope must remain untouched"
    );
    let events: runs::PollRun = runs_proxy
        .call(
            "PollRun",
            (RunControlRequest {
                storage: target.storage,
                run_id: "legacy-foreign-run".into(),
                cursor: 0,
            },),
        )
        .await
        .unwrap();
    assert!(events.done);
    assert_eq!(events.events.last().unwrap().data["status"], "failed");
    assert!(!events.events.iter().any(|event| event.kind == "resumed"));
}

#[tokio::test]
#[ignore = "run through scripts/verify-module-artifact.sh"]
async fn stale_imported_legacy_resume_deletes_only_its_checkpoint_thread() {
    let (_module_host, connection, catalog_proxy, runs_proxy) =
        super::run_lifecycle_tests::module_proxies().await;
    let source = owner(&connection, "legacy-stale-source", None, None, None, None).await;
    let target = owner(&connection, "legacy-stale-target", None, None, None, None).await;
    let graph = parked_graph();
    let source_flow = create_flow_with_graph(
        &catalog_proxy,
        &source.storage,
        "Stale legacy source",
        graph.clone(),
    )
    .await;
    let target_flow = create_flow_with_graph(
        &catalog_proxy,
        &target.storage,
        "Stale legacy target",
        graph.clone(),
    )
    .await;
    let callback = serve_agent(&connection, "legacy-stale-source").await;
    let thread_id = "stale-legacy-checkpoint-thread";
    let mut start = request(&source, source_flow, thread_id, graph, 20_000);
    start.trigger_data = json!({"id":"stale-legacy-item"});
    runs_proxy
        .call::<runs::StartRunResponse>("StartRun", (start,))
        .await
        .unwrap();
    wait_notify(&callback.approval_started, "source approval callback").await;
    poll_until_kind(&runs_proxy, &source.storage, thread_id, "parked").await;
    let source_row: Option<catalog::FlowRun> = catalog_proxy
        .call(
            "GetRun",
            (RunIdRequest {
                storage: source.storage.clone(),
                id: thread_id.into(),
            },),
        )
        .await
        .unwrap();
    let source_row = source_row.unwrap();
    let checkpoints = DriverCheckpointer::<serde_json::Value>::new(source.docs.clone())
        .get_thread(thread_id)
        .await
        .unwrap();
    assert!(!checkpoints.is_empty());

    let mut records = vec![catalog::LegacyImportRecord::Run(
        catalog::LegacyRunRecord {
            id: "legacy-stale-import".into(),
            flow_id: target_flow.clone(),
            thread_id: thread_id.into(),
            status: "pending_approval".into(),
            started_at: source_row.started_at,
            finished_at: source_row.finished_at,
            steps_json: serde_json::to_string(&source_row.steps).unwrap(),
            pending_approvals_json:
                serde_json::to_string(&source_row.pending_approvals).unwrap(),
            error: None,
            graph_hash: Some("deliberately-stale-graph-hash".into()),
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

    let error = runs_proxy
        .call::<runs::ResumeRunResponse>(
            "ResumeRun",
            (runs::ResumeRun {
                run_id: "legacy-stale-import".into(),
                flow_id: target_flow,
                storage: target.storage.clone(),
                caps_host: runs::CapsHost {
                    callback: target.caps,
                },
                approvals: vec!["approval".into()],
                rejections: Vec::new(),
                timeout_ms: 20_000,
            },),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("workflow graph changed"));
    let failed: Option<catalog::FlowRun> = catalog_proxy
        .call(
            "GetRun",
            (RunIdRequest {
                storage: target.storage.clone(),
                id: "legacy-stale-import".into(),
            },),
        )
        .await
        .unwrap();
    assert_eq!(failed.unwrap().status, "cancelled");
    assert!(target_checkpointer_has_no_thread(&target, thread_id).await);
    assert!(target_checkpointer_has_thread(&source, thread_id).await);
}

async fn target_checkpointer_has_thread(owner: &run_artifact_support::Owner, thread: &str) -> bool {
    !DriverCheckpointer::<serde_json::Value>::new(owner.docs.clone())
        .get_thread(thread)
        .await
        .unwrap()
        .is_empty()
}

async fn target_checkpointer_has_no_thread(owner: &run_artifact_support::Owner, thread: &str) -> bool {
    DriverCheckpointer::<serde_json::Value>::new(owner.docs.clone())
        .get_thread(thread)
        .await
        .unwrap()
        .is_empty()
}
