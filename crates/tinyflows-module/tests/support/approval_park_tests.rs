use super::*;
use serde_json::Value;
use tinyflows::graph::checkpoint::Checkpointer as _;
use tinyflows_drivers::{DriverCheckpointer, catalog::FlowCatalogDocuments};

#[tokio::test]
#[ignore = "run through scripts/verify-module-artifact.sh"]
async fn compiled_module_resumes_a_parked_approval_run() {
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
    let owner = owner(&connection, "resume", None, None, None, None).await;
    let graph = parked_graph();
    let flow_id =
        create_flow_with_graph(&catalog_proxy, &owner.storage, "Resume flow", graph.clone()).await;
    let callback = serve_agent(&connection, "resume").await;
    let start = request(&owner, flow_id.clone(), "resume-run", graph, 20_000);
    let started: runs::StartRunResponse = runs_proxy.call("StartRun", (start,)).await.unwrap();
    assert!(!started.already_running);
    wait_notify(&callback.approval_started, "approval callback").await;
    poll_until_kind(&runs_proxy, &owner.storage, "resume-run", "parked").await;

    let resumed: runs::ResumeRunResponse = runs_proxy
        .call(
            "ResumeRun",
            (runs::ResumeRun {
                run_id: "resume-run".into(),
                flow_id,
                storage: owner.storage.clone(),
                caps_host: runs::CapsHost {
                    callback: owner.caps,
                },
                approvals: vec!["approval".into()],
                rejections: vec![],
                timeout_ms: 20_000,
            },),
        )
        .await
        .unwrap();
    assert_eq!(resumed.run_id, "resume-run");
    let terminal = poll_until_done(&runs_proxy, &owner.storage, "resume-run").await;
    assert!(terminal.events.iter().any(|event| event.kind == "resumed"));
    assert!(matches!(
        terminal.events.last().unwrap().data["status"].as_str(),
        Some("completed" | "completed_with_warnings")
    ));
    let row: Option<catalog::FlowRun> = catalog_proxy
        .call(
            "GetRun",
            (RunIdRequest {
                storage: owner.storage,
                id: "resume-run".into(),
            },),
        )
        .await
        .unwrap();
    let row = row.expect("resumed run remains in the owner scope");
    assert_eq!(row.status, terminal.events.last().unwrap().data["status"]);
    assert!(row.pending_approvals.is_empty());
    assert_eq!(
        row.steps.len(),
        3,
        "the resumed gate replaces its parked step"
    );
    let diagnostic_nodes: Vec<&str> = row
        .steps
        .iter()
        .filter(|step| !step.diagnostics.is_empty())
        .map(|step| step.node_id.as_str())
        .collect();
    if row.status == "completed_with_warnings" {
        assert_eq!(diagnostic_nodes, ["dedup"]);
    } else {
        assert!(diagnostic_nodes.is_empty());
    }
    let approval = row
        .steps
        .iter()
        .find(|step| step.node_id == "approval")
        .expect("resumed approval is recorded");
    assert_eq!(approval.status.as_deref(), Some("success"));
    assert!(approval.diagnostics.is_empty());
    assert!(row.steps.iter().any(|step| step.node_id == "dedup"));
    assert!(row.steps.iter().any(|step| step.node_id == "trigger"));
}

#[tokio::test]
#[ignore = "run through scripts/verify-module-artifact.sh"]
async fn pending_approval_is_parked_without_terminal_event_or_dedup_settlement() {
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
    let owner = owner(
        &connection,
        "approval-park",
        None,
        None,
        None,
        Some(Arc::new(std::sync::atomic::AtomicBool::new(true))),
    )
    .await;
    let graph = parked_graph();
    let flow_id =
        create_flow_with_graph(&catalog_proxy, &owner.storage, "Parked flow", graph.clone()).await;
    let callback = serve_agent(&connection, "approval-park").await;
    let mut request = request(&owner, flow_id.clone(), "parked-run", graph, 20_000);
    request.trigger_data = json!({"id":"parked-item","payload":"review"});
    let started: runs::StartRunResponse = runs_proxy.call("StartRun", (request,)).await.unwrap();
    assert!(!started.already_running);
    wait_notify(&callback.approval_started, "approval callback").await;

    let events = poll_until_kind(&runs_proxy, &owner.storage, "parked-run", "parked").await;
    assert!(events.iter().all(|event| event.kind != "finished"));
    let parked_page: runs::PollRun = runs_proxy
        .call(
            "PollRun",
            (RunControlRequest {
                storage: owner.storage.clone(),
                run_id: "parked-run".into(),
                cursor: 0,
            },),
        )
        .await
        .unwrap();
    assert!(!parked_page.done);
    let row: Option<catalog::FlowRun> = catalog_proxy
        .call(
            "GetRun",
            (RunIdRequest {
                storage: owner.storage.clone(),
                id: "parked-run".into(),
            },),
        )
        .await
        .unwrap();
    let row = row.expect("parked run row persists");
    assert_eq!(row.status, "pending_approval");
    assert_eq!(row.pending_approvals, ["approval"]);

    let documents = FlowCatalogDocuments::new(owner.docs.clone());
    let namespace = format!("flow:{flow_id}");
    assert_eq!(
        documents
            .kv_get(&namespace, "dedup:dedup:tentative")
            .await
            .unwrap(),
        Some(json!(["parked-item"]))
    );
    assert_eq!(
        documents
            .kv_get(&namespace, "dedup:dedup:committed")
            .await
            .unwrap(),
        None
    );
    let checkpointer = DriverCheckpointer::<Value>::new(owner.docs.clone());
    assert!(
        checkpointer
            .get("parked-run", None)
            .await
            .unwrap()
            .is_some()
    );

    let cancelled: runs::CancelRunResponse = runs_proxy
        .call(
            "CancelRun",
            (RunControlRequest {
                storage: owner.storage.clone(),
                run_id: "parked-run".into(),
                cursor: 0,
            },),
        )
        .await
        .unwrap();
    assert!(cancelled.signalled);
    let terminal = poll_until_done(&runs_proxy, &owner.storage, "parked-run").await;
    assert_eq!(terminal.events.last().unwrap().data["status"], "cancelled");
    assert_eq!(
        documents
            .kv_get(&namespace, "dedup:dedup:tentative")
            .await
            .unwrap(),
        None
    );
    assert!(
        checkpointer
            .get("parked-run", None)
            .await
            .unwrap()
            .is_some(),
        "a failed best-effort checkpoint deletion does not wedge terminal polling"
    );
    let repeated_cancel: runs::CancelRunResponse = runs_proxy
        .call(
            "CancelRun",
            (RunControlRequest {
                storage: owner.storage.clone(),
                run_id: "parked-run".into(),
                cursor: 0,
            },),
        )
        .await
        .unwrap();
    assert!(!repeated_cancel.signalled);
    let settled: Option<catalog::FlowRun> = catalog_proxy
        .call(
            "GetRun",
            (RunIdRequest {
                storage: owner.storage,
                id: "parked-run".into(),
            },),
        )
        .await
        .unwrap();
    assert_eq!(settled.unwrap().status, "cancelled");
}
