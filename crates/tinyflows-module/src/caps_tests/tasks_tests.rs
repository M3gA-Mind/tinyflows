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
    let barrier = support::serve_caps_with_nested_barrier(&host, "alice", "/caps/alice").await;
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
            {"id":"spawn","name":"Spawn","kind":"spawn","config":{"target":"tool","slug":"nested.blocked","args":{"source":"nested"}}},
            {"id":"release","name":"Release","kind":"tool_call","config":{"slug":"nested.release","args":{}}},
            {"id":"gate","name":"Gate","kind":"gate","config":{"from":["spawn"],"poll_interval_ms":1,"max_polls":100,"on_timeout":"error"}}
        ],
        "edges": [
            {"from_node":"trigger","from_port":"main","to_node":"spawn","to_port":"main"},
            {"from_node":"spawn","from_port":"main","to_node":"gate","to_port":"main"},
            {"from_node":"spawn","from_port":"main","to_node":"release","to_port":"main"}
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
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        await_task(runner.as_ref(), &ticket),
    )
    .await
    .expect("release node should run while nested tool callback is blocked");
    let TaskState::Done(result) = result else {
        panic!("child workflow should return a real result")
    };
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        barrier.started.notified(),
    )
    .await
    .expect("nested tool must start before the child workflow settles");
    let encoded = result.to_string();
    assert!(
        encoded.contains("nested.blocked"),
        "nested task result missing: {result}"
    );
    assert!(
        encoded.contains("nested.release"),
        "release result missing: {result}"
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

#[tokio::test]
async fn parked_child_workflow_fails_explicitly_before_downstream_actions() {
    let (module, host) = support::memory_bus().await;
    let calls = support::serve_caps(&host, "alice", "/caps/alice", false).await;
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
            {"id":"review","name":"Review","kind":"approval","config":{"request_id":"child-approval","title":"Proceed?"}},
            {"id":"after","name":"After approval","kind":"tool_call","config":{"slug":"nested.after-approval","args":{}}}
        ],
        "edges": [
            {"from_node":"trigger","from_port":"main","to_node":"review","to_port":"main"},
            {"from_node":"review","from_port":"approved","to_node":"after","to_port":"main"}
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
    let TaskState::Failed(message) = await_task(runner.as_ref(), &ticket).await else {
        panic!("a parked child cannot be reported as a completed task")
    };
    assert!(message.contains("waiting for approvals"), "{message}");
    assert!(message.contains("child-approval") || message.contains("review"));
    assert!(
        !calls
            .lock()
            .unwrap()
            .iter()
            .any(|call| call == "alice:ToolInvoke")
    );
}
