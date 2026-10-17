use super::*;
use tinyflows_bus::catalog::RunIdRequest;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tokio::sync::Notify;

#[path = "run_artifact_poll.rs"]
mod run_artifact_poll;
use run_artifact_poll::{poll_until_done, poll_until_kind};
#[path = "run_artifact_support.rs"]
mod run_artifact_support;
use run_artifact_support::*;

#[tokio::test]
#[ignore = "run through scripts/verify-module-artifact.sh"]
async fn compiled_module_runs_scoped_graphs_and_streams_persisted_progress() {
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

    let terminal_write_started = Arc::new(Notify::new());
    let release_terminal_write = Arc::new(Notify::new());
    let alice = owner(
        &connection,
        "alice",
        Some((
            terminal_write_started.clone(),
            release_terminal_write.clone(),
        )),
        None,
    )
    .await;
    let bob = owner(&connection, "bob", None, None).await;
    let alice_flow = create_flow(&catalog_proxy, &alice.storage, "Alice").await;
    let bob_flow = create_flow(&catalog_proxy, &bob.storage, "Bob").await;
    let alice_agent = serve_agent(&connection, "alice").await;
    let bob_agent = serve_agent(&connection, "bob").await;
    let run_id = "same-opaque-run-id";
    let alice_start = request(&alice, alice_flow, run_id, graph(true), 20_000);
    let bob_start = request(&bob, bob_flow, run_id, graph(true), 20_000);

    let invalid = request(
        &alice,
        alice_start.context.flow_id.clone(),
        "preflight",
        graph(false),
        20_000,
    );
    let error = runs_proxy
        .call::<runs::StartRunResponse>("StartRun", (invalid,))
        .await;
    assert!(
        error.is_err(),
        "missing declared input must reject synchronously"
    );
    let missing = catalog_proxy
        .call::<Option<catalog::FlowRun>>(
            "GetRun",
            (RunIdRequest {
                storage: alice.storage.clone(),
                id: "preflight".into(),
            },),
        )
        .await;
    assert_eq!(
        missing.unwrap(),
        None,
        "preflight failure must leave no row"
    );

    let started: runs::StartRunResponse = runs_proxy
        .call("StartRun", (alice_start.clone(),))
        .await
        .unwrap();
    assert_eq!(started.run_id, run_id);
    assert!(!started.already_running);
    alice_agent.started.notified().await;
    let live_graph = poll_until_kind(&runs_proxy, &alice.storage, run_id, "graph_observation").await;
    assert!(live_graph.iter().any(|event| event.graph_run_id.is_some()));

    let duplicate: runs::StartRunResponse = runs_proxy
        .call("StartRun", (alice_start.clone(),))
        .await
        .unwrap();
    assert!(duplicate.already_running);
    assert_eq!(alice_agent.calls.load(Ordering::SeqCst), 1);

    let bob_started: runs::StartRunResponse =
        runs_proxy.call("StartRun", (bob_start,)).await.unwrap();
    assert!(!bob_started.already_running);
    bob_agent.started.notified().await;
    assert_eq!(bob_agent.calls.load(Ordering::SeqCst), 1);

    let wrong_owner = RunControlRequest {
        storage: owner(&connection, "mallory", None, None).await.storage,
        run_id: run_id.into(),
        cursor: 0,
    };
    assert!(
        runs_proxy
            .call::<runs::PollRun>("PollRun", (wrong_owner,))
            .await
            .is_err()
    );

    let alice_cancel: runs::CancelRunResponse = runs_proxy
        .call(
            "CancelRun",
            (RunControlRequest {
                storage: alice.storage.clone(),
                run_id: run_id.into(),
                cursor: 0,
            },),
        )
        .await
        .unwrap();
    assert!(alice_cancel.signalled);
    alice_agent.release.notify_one();
    bob_agent.release.notify_one();

    terminal_write_started.notified().await;
    let gap: runs::PollRun = runs_proxy
        .call(
            "PollRun",
            (RunControlRequest {
                storage: alice.storage.clone(),
                run_id: run_id.into(),
                cursor: 0,
            },),
        )
        .await
        .unwrap();
    assert!(!gap.done, "terminal row cannot finish polling before its event");
    assert!(gap.events.iter().all(|event| event.kind != "finished"));
    release_terminal_write.notify_one();

    let alice_poll = poll_until_done(&runs_proxy, &alice.storage, run_id).await;
    let bob_poll = poll_until_done(&runs_proxy, &bob.storage, run_id).await;
    assert!(
        alice_poll
            .events
            .iter()
            .any(|event| event.kind == "agent_progress")
    );
    assert!(
        alice_poll
            .events
            .iter()
            .any(|event| event.kind == "graph_observation")
    );
    assert!(alice_poll.events.iter().any(|event| event.kind == "step"));
    assert_eq!(
        alice_poll.events.last().unwrap().data["status"],
        "cancelled"
    );
    assert!(bob_poll.events.iter().any(|event| event.kind == "finished"));

    let alice_row: Option<catalog::FlowRun> = catalog_proxy
        .call(
            "GetRun",
            (RunIdRequest {
                storage: alice.storage.clone(),
                id: run_id.into(),
            },),
        )
        .await
        .unwrap();
    let bob_row: Option<catalog::FlowRun> = catalog_proxy
        .call(
            "GetRun",
            (RunIdRequest {
                storage: bob.storage.clone(),
                id: run_id.into(),
            },),
        )
        .await
        .unwrap();
    let alice_row = alice_row.expect("alice run row persisted");
    let bob_row = bob_row.expect("bob run row persisted");
    assert_eq!(alice_row.status, "cancelled");
    assert_eq!(bob_row.status, "completed");
    assert!(alice_row.steps.iter().any(|step| step.node_id == "agent"));
    assert!(bob_row.steps.iter().any(|step| step.node_id == "agent"));
    let output = bob_row
        .steps
        .iter()
        .find(|step| step.node_id == "output")
        .expect("downstream transform ran");
    assert_eq!(output.output[0]["json"]["declared"], "declared-value");
    assert_eq!(output.output[0]["json"]["payload"], "trigger-value");
    let terminal_cancel: runs::CancelRunResponse = runs_proxy
        .call(
            "CancelRun",
            (RunControlRequest {
                storage: alice.storage.clone(),
                run_id: run_id.into(),
                cursor: 0,
            },),
        )
        .await
        .unwrap();
    assert!(!terminal_cancel.signalled);

    let insert_started = Arc::new(Notify::new());
    let release_insert = Arc::new(Notify::new());
    let fail_insert = Arc::new(AtomicBool::new(true));
    let retry_owner = owner(
        &connection,
        "retry",
        None,
        Some((
            fail_insert,
            insert_started.clone(),
            release_insert.clone(),
        )),
    )
    .await;
    let retry_flow = create_flow(&catalog_proxy, &retry_owner.storage, "Retry").await;
    let retry_agent = serve_agent(&connection, "retry").await;
    let retry_request = request(&retry_owner, retry_flow, "rollback-id", graph(true), 20_000);
    let retry_proxy = connection
        .proxy(BUS_NAME, runs::OBJECT_PATH, runs::INTERFACE)
        .unwrap();
    let first_attempt = {
        let request = retry_request.clone();
        tokio::spawn(async move {
            retry_proxy
                .call::<runs::StartRunResponse>("StartRun", (request,))
                .await
        })
    };
    insert_started.notified().await;
    release_insert.notify_one();
    assert!(first_attempt.await.unwrap().is_err());
    let failed_row: Option<catalog::FlowRun> = catalog_proxy
        .call(
            "GetRun",
            (RunIdRequest {
                storage: retry_owner.storage.clone(),
                id: "rollback-id".into(),
            },),
        )
        .await
        .unwrap();
    assert_eq!(failed_row, None);
    let retry_started: runs::StartRunResponse = runs_proxy
        .call("StartRun", (retry_request,))
        .await
        .unwrap();
    assert!(!retry_started.already_running);
    retry_agent.started.notified().await;
    retry_agent.release.notify_one();
    let _ = poll_until_done(&runs_proxy, &retry_owner.storage, "rollback-id").await;
}

