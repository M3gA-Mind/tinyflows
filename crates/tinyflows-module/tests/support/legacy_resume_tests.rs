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
    start.trigger_data = json!({"id":"legacy-item","payload":"checkpoint state"});
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
async fn transient_legacy_checkpoint_read_error_leaves_the_parked_row_retryable() {
    let (_module_host, connection, catalog_proxy, runs_proxy) =
        super::run_lifecycle_tests::module_proxies().await;
    let (owner, fail_checkpoint_query) =
        owner_with_checkpoint_query_failure(&connection, "legacy-checkpoint-retry").await;
    let graph = parked_graph();
    let flow_id = create_flow_with_graph(
        &catalog_proxy,
        &owner.storage,
        "Legacy checkpoint retry",
        graph.clone(),
    )
    .await;
    let callback = serve_agent(&connection, "legacy-checkpoint-retry").await;
    let mut start = request(&owner, flow_id.clone(), "legacy-retry-run", graph, 20_000);
    start.trigger_data = json!({"id":"legacy-retry-item"});
    runs_proxy
        .call::<runs::StartRunResponse>("StartRun", (start,))
        .await
        .unwrap();
    wait_notify(&callback.approval_started, "approval callback").await;
    poll_until_kind(&runs_proxy, &owner.storage, "legacy-retry-run", "parked").await;
    let stored = owner
        .docs
        .get("flows.runs", "legacy-retry-run")
        .await
        .unwrap()
        .unwrap();
    let mut legacy = stored.doc.clone();
    legacy.as_object_mut().unwrap().remove("execution_snapshot");
    owner
        .docs
        .put("flows.runs", "legacy-retry-run", legacy, stored.unchanged())
        .await
        .unwrap();

    fail_checkpoint_query.store(true, Ordering::SeqCst);
    let resume = runs::ResumeRun {
        run_id: "legacy-retry-run".into(),
        flow_id,
        storage: owner.storage.clone(),
        caps_host: runs::CapsHost {
            callback: owner.caps.clone(),
        },
        approvals: vec!["approval".into()],
        rejections: Vec::new(),
        timeout_ms: 20_000,
    };
    let error = runs_proxy
        .call::<runs::ResumeRunResponse>("ResumeRun", (resume.clone(),))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("reading legacy parked run checkpoint"));
    let parked: Option<catalog::FlowRun> = catalog_proxy
        .call(
            "GetRun",
            (RunIdRequest {
                storage: owner.storage.clone(),
                id: "legacy-retry-run".into(),
            },),
        )
        .await
        .unwrap();
    assert_eq!(parked.unwrap().status, "pending_approval");
    assert!(target_checkpointer_has_thread(&owner, "legacy-retry-run").await);

    runs_proxy
        .call::<runs::ResumeRunResponse>("ResumeRun", (resume,))
        .await
        .unwrap();
    let terminal = poll_until_done(&runs_proxy, &owner.storage, "legacy-retry-run").await;
    assert_eq!(terminal.events.last().unwrap().data["status"], "completed");
}

#[tokio::test]
#[ignore = "run through scripts/verify-module-artifact.sh"]
async fn legacy_resume_marker_survives_repark_and_continues_again() {
    let (_module_host, connection, catalog_proxy, runs_proxy) =
        super::run_lifecycle_tests::module_proxies().await;
    let owner = owner(&connection, "legacy-repark", None, None, None, None).await;
    let graph = json!({
        "inputs":[{"name":"declared","type":"string","required":true}],
        "nodes":[
            {"id":"trigger","name":"Trigger","kind":"trigger","config":{}},
            {"id":"first","name":"First approval","kind":"approval","config":{"request_id":"first-review"}},
            {"id":"second","name":"Second approval","kind":"approval","config":{"request_id":"second-review"}},
            {"id":"done","name":"Done","kind":"transform","config":{"set":{"done":true}}}
        ],
        "edges":[
            {"from_node":"trigger","from_port":"main","to_node":"first","to_port":"main"},
            {"from_node":"first","from_port":"approved","to_node":"second","to_port":"main"},
            {"from_node":"second","from_port":"approved","to_node":"done","to_port":"main"}
        ]
    });
    let flow_id = create_flow_with_graph(
        &catalog_proxy,
        &owner.storage,
        "Legacy multiple approval",
        graph.clone(),
    )
    .await;
    let callback = serve_agent(&connection, "legacy-repark").await;
    let mut start = request(&owner, flow_id.clone(), "legacy-repark-run", graph, 20_000);
    start.trigger_data = json!({"id":"repark-item"});
    runs_proxy
        .call::<runs::StartRunResponse>("StartRun", (start,))
        .await
        .unwrap();
    wait_notify(&callback.approval_started, "first approval callback").await;
    poll_until_kind(&runs_proxy, &owner.storage, "legacy-repark-run", "parked").await;
    let stored = owner
        .docs
        .get("flows.runs", "legacy-repark-run")
        .await
        .unwrap()
        .unwrap();
    let mut legacy = stored.doc.clone();
    legacy.as_object_mut().unwrap().remove("execution_snapshot");
    owner
        .docs
        .put("flows.runs", "legacy-repark-run", legacy, stored.unchanged())
        .await
        .unwrap();

    runs_proxy
        .call::<runs::ResumeRunResponse>(
            "ResumeRun",
            (runs::ResumeRun {
                run_id: "legacy-repark-run".into(),
                flow_id: flow_id.clone(),
                storage: owner.storage.clone(),
                caps_host: runs::CapsHost {
                    callback: owner.caps.clone(),
                },
                approvals: vec!["first".into()],
                rejections: Vec::new(),
                timeout_ms: 20_000,
            },),
        )
        .await
        .unwrap();
    let first_park = poll_until_parked_count(&runs_proxy, &owner.storage, "legacy-repark-run", 2).await;
    let marker = owner
        .docs
        .get("flows.runs", "legacy-repark-run")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(marker.doc["status"], "pending_approval");
    assert_eq!(marker.doc["execution_snapshot"]["legacy_resume"], true);
    assert_eq!(marker.doc["execution_snapshot"]["thread_id"], "legacy-repark-run");
    assert!(first_park.events.iter().any(|event| event.kind == "resumed"));

    runs_proxy
        .call::<runs::ResumeRunResponse>(
            "ResumeRun",
            (runs::ResumeRun {
                run_id: "legacy-repark-run".into(),
                flow_id,
                storage: owner.storage.clone(),
                caps_host: runs::CapsHost {
                    callback: owner.caps,
                },
                approvals: vec!["second".into()],
                rejections: Vec::new(),
                timeout_ms: 20_000,
            },),
        )
        .await
        .unwrap();
    let terminal = poll_until_done(&runs_proxy, &owner.storage, "legacy-repark-run").await;
    assert_eq!(terminal.events.last().unwrap().data["status"], "completed");
    assert_eq!(
        terminal.events.iter().filter(|event| event.kind == "finished").count(),
        1
    );
}

async fn poll_until_parked_count(
    proxy: &tinybus::Proxy,
    storage: &catalog::CatalogStorage,
    run_id: &str,
    count: usize,
) -> runs::PollRun {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let mut cursor = 0;
        let mut events = Vec::new();
        loop {
            let page: runs::PollRun = proxy
                .call(
                    "PollRun",
                    (RunControlRequest {
                        storage: storage.clone(),
                        run_id: run_id.into(),
                        cursor,
                    },),
                )
                .await
                .unwrap();
            cursor = page.next_cursor;
            events.extend(page.events);
            if events.iter().filter(|event| event.kind == "parked").count() >= count {
                return runs::PollRun {
                    events,
                    next_cursor: cursor,
                    done: page.done,
                };
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("a legacy resumed run should reach its next approval park")
}

