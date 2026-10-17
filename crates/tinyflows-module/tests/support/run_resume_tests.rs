use super::run_lifecycle_tests::module_proxies;
use super::*;

#[tokio::test]
#[ignore = "run through scripts/verify-module-artifact.sh"]
async fn compiled_module_resumes_a_rejected_approval_over_the_runs_bus() {
    let (_module_host, connection, catalog_proxy, runs_proxy) = module_proxies().await;
    let owner = owner(&connection, "rejection-resume", None, None, None, None).await;
    let graph = json!({
        "inputs":[{"name":"declared","type":"string","required":true}],
        "nodes":[
            {"id":"trigger","name":"Trigger","kind":"trigger","config":{}},
            {"id":"approval","name":"Approval","kind":"approval","config":{"request_id":"reject-me"}},
            {"id":"accepted","name":"Accepted","kind":"transform","config":{"set":{"approved":true}}},
            {"id":"recovery","name":"Recovery","kind":"transform","config":{"set":{"recovered":true}}}
        ],
        "edges":[
            {"from_node":"trigger","from_port":"main","to_node":"approval","to_port":"main"},
            {"from_node":"approval","from_port":"main","to_node":"accepted","to_port":"main"},
            {"from_node":"approval","from_port":"error","to_node":"recovery","to_port":"main"}
        ]
    });
    let flow_id = create_flow_with_graph(
        &catalog_proxy,
        &owner.storage,
        "Reject resume",
        graph.clone(),
    )
    .await;
    let callback = serve_agent(&connection, "rejection-resume").await;
    let start = request(&owner, flow_id.clone(), "rejection-run", graph, 20_000);
    runs_proxy
        .call::<runs::StartRunResponse>("StartRun", (start,))
        .await
        .unwrap();
    wait_notify(&callback.approval_started, "approval callback").await;
    poll_until_kind(&runs_proxy, &owner.storage, "rejection-run", "parked").await;

    let response: runs::ResumeRunResponse = runs_proxy
        .call(
            "ResumeRun",
            (runs::ResumeRun {
                run_id: "rejection-run".into(),
                flow_id,
                storage: owner.storage.clone(),
                caps_host: runs::CapsHost {
                    callback: owner.caps.clone(),
                },
                approvals: Vec::new(),
                rejections: vec!["approval".into()],
                timeout_ms: 20_000,
            },),
        )
        .await
        .unwrap();
    assert_eq!(response.run_id, "rejection-run");
    let terminal = poll_until_done(&runs_proxy, &owner.storage, "rejection-run").await;
    assert_eq!(terminal.events.last().unwrap().data["status"], "completed");
    let row: Option<catalog::FlowRun> = catalog_proxy
        .call(
            "GetRun",
            (RunIdRequest {
                storage: owner.storage,
                id: "rejection-run".into(),
            },),
        )
        .await
        .unwrap();
    let row = row.unwrap();
    assert!(row.steps.iter().any(|step| step.node_id == "recovery"));
    assert!(!row.steps.iter().any(|step| step.node_id == "accepted"));
}

#[tokio::test]
#[ignore = "run through scripts/verify-module-artifact.sh"]
async fn compiled_module_resumes_an_ordinary_nested_workflow_approval() {
    let (_module_host, connection, catalog_proxy, runs_proxy) = module_proxies().await;
    let owner = owner(&connection, "nested-resume", None, None, None, None).await;
    let child_graph = json!({
        "nodes":[
            {"id":"trigger","name":"Trigger","kind":"trigger","config":{}},
            {"id":"gate","name":"Gate","kind":"approval","config":{"request_id":"nested-review"}},
            {"id":"child-output","name":"Child output","kind":"transform","config":{"set":{"done":true}}}
        ],
        "edges":[
            {"from_node":"trigger","from_port":"main","to_node":"gate","to_port":"main"},
            {"from_node":"gate","from_port":"main","to_node":"child-output","to_port":"main"}
        ]
    });
    let child_id = create_flow_with_graph(
        &catalog_proxy,
        &owner.storage,
        "Child approval",
        child_graph,
    )
    .await;
    let parent_graph = json!({
        "inputs":[{"name":"declared","type":"string","required":true}],
        "nodes":[
            {"id":"trigger","name":"Trigger","kind":"trigger","config":{}},
            {"id":"child","name":"Child","kind":"sub_workflow","config":{"workflow_id":child_id}}
        ],
        "edges":[
            {"from_node":"trigger","from_port":"main","to_node":"child","to_port":"main"}
        ]
    });
    let parent_id = create_flow_with_graph(
        &catalog_proxy,
        &owner.storage,
        "Parent approval",
        parent_graph.clone(),
    )
    .await;
    let callback = serve_agent(&connection, "nested-resume").await;
    let start = request(
        &owner,
        parent_id.clone(),
        "nested-run",
        parent_graph,
        20_000,
    );
    runs_proxy
        .call::<runs::StartRunResponse>("StartRun", (start,))
        .await
        .unwrap();
    wait_notify(&callback.approval_started, "nested approval callback").await;
    poll_until_kind(&runs_proxy, &owner.storage, "nested-run", "parked").await;
    let parked: Option<catalog::FlowRun> = catalog_proxy
        .call(
            "GetRun",
            (RunIdRequest {
                storage: owner.storage.clone(),
                id: "nested-run".into(),
            },),
        )
        .await
        .unwrap();
    assert_eq!(parked.unwrap().pending_approvals, ["child::gate"]);

    runs_proxy
        .call::<runs::ResumeRunResponse>(
            "ResumeRun",
            (runs::ResumeRun {
                run_id: "nested-run".into(),
                flow_id: parent_id,
                storage: owner.storage.clone(),
                caps_host: runs::CapsHost {
                    callback: owner.caps,
                },
                approvals: vec!["child::gate".into()],
                rejections: Vec::new(),
                timeout_ms: 20_000,
            },),
        )
        .await
        .unwrap();
    let terminal = poll_until_done(&runs_proxy, &owner.storage, "nested-run").await;
    assert_eq!(terminal.events.last().unwrap().data["status"], "completed");
    let row: Option<catalog::FlowRun> = catalog_proxy
        .call(
            "GetRun",
            (RunIdRequest {
                storage: owner.storage,
                id: "nested-run".into(),
            },),
        )
        .await
        .unwrap();
    assert!(
        row.unwrap()
            .steps
            .iter()
            .any(|step| step.node_id == "child" && step.status.as_deref() == Some("success"))
    );
}
