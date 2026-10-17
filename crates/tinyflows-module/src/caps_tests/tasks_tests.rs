use super::*;

use serde_json::json;
use tinyflows::caps::{TaskRunner, TaskSpec, TaskState};

async fn await_task(runner: &dyn TaskRunner, ticket: &str) -> TaskState {
    for _ in 0..20_000 {
        let state = runner.poll(ticket).await.unwrap();
        if state.is_settled() {
            return state;
        }
        tokio::task::yield_now().await;
    }
    panic!("capability-backed task {ticket:?} did not settle")
}

#[tokio::test]
async fn spawned_tool_and_http_tasks_return_host_results() {
    let (module, host) = support::memory_bus().await;
    support::serve_caps(&host, "alice", "/caps/alice", false).await;
    host.request_name("test.CatalogStorage").await.unwrap();
    let (storage, _) = super::scope_tests::storage_callback(&host, "alice").await;
    let capabilities = CapsHostPorts::capabilities(
        module,
        support::caps_host("/caps/alice"),
        RunScope {
            flow_id: "flow-1".into(),
            storage,
        },
    )
    .await
    .unwrap();
    let runner = capabilities.tasks.as_ref().unwrap();

    let tool = runner
        .start(TaskSpec::Tool {
            slug: "child.tool".into(),
            args: json!({"x":1}),
        })
        .await
        .unwrap();
    assert_eq!(
        await_task(runner.as_ref(), &tool).await,
        TaskState::Done(json!({"owner":"alice","slug":"child.tool","args":{"x":1},"conn":null}))
    );
    let http = runner
        .start(TaskSpec::Http {
            request: json!({"url":"https://example.test"}),
        })
        .await
        .unwrap();
    assert_eq!(
        await_task(runner.as_ref(), &http).await,
        TaskState::Done(
            json!({"owner":"alice","request":{"url":"https://example.test"},"conn":null})
        )
    );
}

#[tokio::test]
async fn spawned_child_workflow_keeps_the_runner_for_its_nested_spawn_and_gate() {
    let (module, host) = support::memory_bus().await;
    support::serve_caps(&host, "alice", "/caps/alice", false).await;
    host.request_name("test.CatalogStorage").await.unwrap();
    let (storage, _) = super::scope_tests::storage_callback(&host, "alice").await;
    let capabilities = CapsHostPorts::capabilities(
        module,
        support::caps_host("/caps/alice"),
        RunScope {
            flow_id: "flow-1".into(),
            storage,
        },
    )
    .await
    .unwrap();
    let child = json!({
        "nodes": [
            {"id":"trigger","name":"Trigger","kind":"trigger","config":{}},
            {"id":"spawn","name":"Spawn","kind":"spawn","config":{"target":"tool","slug":"nested.tool","args":{"source":"nested"}}},
            {"id":"gate","name":"Gate","kind":"gate","config":{"from":["spawn"],"poll_interval_ms":1,"max_polls":100,"on_timeout":"error"}}
        ],
        "edges": [
            {"from_node":"trigger","from_port":"main","to_node":"spawn","to_port":"main"},
            {"from_node":"spawn","from_port":"main","to_node":"gate","to_port":"main"}
        ]
    });
    let runner = capabilities.tasks.as_ref().unwrap();
    let ticket = runner
        .start(TaskSpec::Workflow {
            graph: child,
            input: json!({}),
        })
        .await
        .unwrap();
    let TaskState::Done(result) = await_task(runner.as_ref(), &ticket).await else {
        panic!("child workflow should return a real result")
    };
    let encoded = result.to_string();
    assert!(
        encoded.contains("nested.tool"),
        "nested task result missing: {result}"
    );
    assert!(
        encoded.contains("alice"),
        "nested host callback missing: {result}"
    );
    assert!(
        !encoded.contains("\"spec\":\"workflow\""),
        "runner echoed the TaskSpec: {result}"
    );
}

#[tokio::test]
async fn task_runner_cancellation_settles_an_in_flight_callback_ticket() {
    let (module, host) = support::memory_bus().await;
    let gate = support::serve_caps_with_blocked_tools(&host, "alice", "/caps/alice").await;
    host.request_name("test.CatalogStorage").await.unwrap();
    let (storage, _) = super::scope_tests::storage_callback(&host, "alice").await;
    let capabilities = CapsHostPorts::capabilities(
        module,
        support::caps_host("/caps/alice"),
        RunScope {
            flow_id: "flow-1".into(),
            storage,
        },
    )
    .await
    .unwrap();
    let runner = capabilities.tasks.as_ref().unwrap();
    let ticket = runner
        .start(TaskSpec::Tool {
            slug: "blocked".into(),
            args: json!({}),
        })
        .await
        .unwrap();
    gate.started.notified().await;
    runner.cancel(&ticket).await.unwrap();
    gate.release.notify_one();
    let state = runner.poll(&ticket).await.unwrap();
    assert_eq!(state, TaskState::Failed("cancelled".into()));
    assert_eq!(runner.poll(&ticket).await.unwrap(), state);
}

#[tokio::test]
async fn inline_child_workflow_refuses_legacy_unsupported_engine_topology() {
    let (module, host) = support::memory_bus().await;
    support::serve_caps(&host, "alice", "/caps/alice", false).await;
    host.request_name("test.CatalogStorage").await.unwrap();
    let (storage, _) = super::scope_tests::storage_callback(&host, "alice").await;
    let capabilities = CapsHostPorts::capabilities(
        module,
        support::caps_host("/caps/alice"),
        RunScope {
            flow_id: "flow-1".into(),
            storage,
        },
    )
    .await
    .unwrap();
    let runner = capabilities.tasks.as_ref().unwrap();
    let ticket = runner
        .start(TaskSpec::Workflow {
            graph: support::unsafe_child_graph(),
            input: json!({}),
        })
        .await
        .unwrap();
    let TaskState::Failed(message) = await_task(runner.as_ref(), &ticket).await else {
        panic!("unsupported topology must not run")
    };
    assert!(message.contains("unsupported_nested_conditional_fan_in"));
}
