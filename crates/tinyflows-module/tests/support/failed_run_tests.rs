use super::*;
use tinyflows_drivers::catalog::FlowCatalogDocuments;

#[tokio::test]
#[ignore = "run through scripts/verify-module-artifact.sh"]
async fn start_run_reports_the_missing_declared_input_at_the_bus_boundary() {
    let (_module_host, connection, catalog_proxy, runs_proxy) = module_proxies().await;
    let owner = owner(&connection, "missing-repo", None, None, None, None).await;
    let graph = serde_json::json!({
        "inputs": [{"name":"repo", "type":"string", "required":true}],
        "nodes": [{"id":"trigger", "name":"Trigger", "kind":"trigger", "config":{}}],
        "edges": []
    });
    let flow_id = create_flow_with_graph(&catalog_proxy, &owner.storage, "Missing repo", graph.clone()).await;
    let mut request = request(&owner, flow_id, "missing-repo-run", graph, 20_000);
    request.inputs.clear();

    let error = runs_proxy
        .call::<runs::StartRunResponse>("StartRun", (request,))
        .await
        .expect_err("missing required workflow input must reject StartRun");
    let message = error.to_string();
    assert!(message.contains("repo"), "input field name was lost: {message}");
    assert!(
        message.contains("missing required input"),
        "actionable validation cause was lost: {message}"
    );

    let row: Option<catalog::FlowRun> = catalog_proxy
        .call(
            "GetRun",
            (RunIdRequest {
                storage: owner.storage,
                id: "missing-repo-run".into(),
            },),
        )
        .await
        .unwrap();
    assert_eq!(row, None, "preflight failure must not create a run row");
}

#[tokio::test]
#[ignore = "run through scripts/verify-module-artifact.sh"]
async fn failed_actor_persists_graph_observations_and_settles_executed_dedup_nodes() {
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
    let owner = owner(&connection, "failed-run", None, None, None, None).await;
    let graph = dedup_graph("failure-dedup");
    let flow_id =
        create_flow_with_graph(&catalog_proxy, &owner.storage, "Failed flow", graph.clone()).await;
    let _agent = serve_failing_agent(&connection, "failed-run").await;
    let mut request = request(&owner, flow_id.clone(), "failed-run-id", graph, 20_000);
    request.trigger_data = json!({"id":"retry-item"});
    let started: runs::StartRunResponse = runs_proxy.call("StartRun", (request,)).await.unwrap();
    assert!(!started.already_running);

    let events = poll_until_done(&runs_proxy, &owner.storage, "failed-run-id").await;
    assert!(
        events
            .events
            .iter()
            .any(|event| event.kind == "graph_observation" && event.graph_run_id.is_some())
    );
    assert_eq!(events.events.last().unwrap().data["status"], "failed");
    assert!(
        events.events.last().unwrap().data.get("output").is_none(),
        "a failed engine call has no fabricated outcome"
    );
    let finished: runs::RunFinished =
        serde_json::from_value(events.events.last().unwrap().data.clone()).unwrap();
    assert!(!finished.successful);
    let row: Option<catalog::FlowRun> = catalog_proxy
        .call(
            "GetRun",
            (RunIdRequest {
                storage: owner.storage.clone(),
                id: "failed-run-id".into(),
            },),
        )
        .await
        .unwrap();
    let row = row.expect("failed run row persists");
    assert!(
        row.error
            .unwrap()
            .contains("injected agent callback failure")
    );
    let summary: Option<catalog::Workflow> = catalog_proxy
        .call("GetWorkflow", &(owner.storage.clone(), flow_id.clone()))
        .await
        .unwrap();
    assert_eq!(
        summary.unwrap().last_status.as_deref(),
        Some("failed"),
        "failed actor settlement updates the workflow summary"
    );

    let catalog_docs = FlowCatalogDocuments::new(owner.docs.clone());
    let namespace = format!("flow:{flow_id}");
    assert_eq!(
        catalog_docs
            .kv_get(&namespace, "dedup:failure-dedup:tentative")
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        catalog_docs
            .kv_get(&namespace, "dedup:failure-dedup:committed")
            .await
            .unwrap(),
        None
    );
}
