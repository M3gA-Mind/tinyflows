use super::*;
use serde_json::json;

struct ValueExecutor(Value);

#[async_trait]
impl TaskExecutor for ValueExecutor {
    async fn execute(&self, _spec: TaskSpec) -> Result<Value> {
        Ok(self.0.clone())
    }
}

struct FailedExecutor;

#[async_trait]
impl TaskExecutor for FailedExecutor {
    async fn execute(&self, _spec: TaskSpec) -> Result<Value> {
        Err(EngineError::Capability("host action refused".into()))
    }
}

struct PanickingExecutor;

#[async_trait]
impl TaskExecutor for PanickingExecutor {
    async fn execute(&self, _spec: TaskSpec) -> Result<Value> {
        panic!("executor panic must be captured")
    }
}

struct BlockingExecutor {
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

#[async_trait]
impl TaskExecutor for BlockingExecutor {
    async fn execute(&self, _spec: TaskSpec) -> Result<Value> {
        self.started.notify_one();
        self.release.notified().await;
        Ok(json!("late"))
    }
}

async fn settled(runner: &TokioTaskRunner, ticket: &str) -> TaskState {
    for _ in 0..128 {
        let state = runner.poll(ticket).await.unwrap();
        if state.is_settled() {
            return state;
        }
        tokio::task::yield_now().await;
    }
    panic!("task did not settle")
}

fn spec() -> TaskSpec {
    TaskSpec::Tool {
        slug: "demo.run".to_string(),
        args: json!({ "x": 1 }),
    }
}

#[tokio::test]
async fn a_started_task_settles_and_can_be_polled_repeatedly() {
    let runner = TokioTaskRunner::new();
    let ticket = runner.start(spec()).await.expect("start");

    // Poll until settled; the gate does exactly this, once per activation.
    let mut state = runner.poll(&ticket).await.expect("poll");
    for _ in 0..64 {
        if state.is_settled() {
            break;
        }
        tokio::task::yield_now().await;
        state = runner.poll(&ticket).await.expect("poll");
    }
    assert!(
        matches!(state, TaskState::Done(_)),
        "task should settle, got {state:?}"
    );
    // Polling again must not consume the result — a gate may see the same
    // ticket on several activations before it releases.
    assert_eq!(runner.poll(&ticket).await.expect("re-poll"), state);
}

#[tokio::test]
async fn tickets_are_unique() {
    let runner = TokioTaskRunner::new();
    let a = runner.start(spec()).await.expect("start");
    let b = runner.start(spec()).await.expect("start");
    assert_ne!(a, b);
}

#[tokio::test]
async fn an_unknown_ticket_is_an_error_rather_than_a_silent_pending() {
    let runner = TokioTaskRunner::new();
    assert!(runner.poll("task-999").await.is_err());
    assert!(runner.cancel("task-999").await.is_err());
}

#[tokio::test]
async fn cancelling_settles_the_task_as_failed() {
    let runner = TokioTaskRunner::new();
    let ticket = runner.start(spec()).await.expect("start");
    runner.cancel(&ticket).await.expect("cancel");
    let state = runner.poll(&ticket).await.expect("poll");
    assert!(state.is_settled(), "a cancelled task must not stay pending");
}

/// Cancelling something that already finished must not rewrite its result —
/// a gate that released on it has already used that value.
#[tokio::test]
async fn cancelling_a_settled_task_leaves_its_result_alone() {
    let runner = TokioTaskRunner::new();
    let ticket = runner.start(spec()).await.expect("start");
    for _ in 0..64 {
        if runner.poll(&ticket).await.expect("poll").is_settled() {
            break;
        }
        tokio::task::yield_now().await;
    }
    let before = runner.poll(&ticket).await.expect("poll");
    runner.cancel(&ticket).await.expect("cancel");
    assert_eq!(runner.poll(&ticket).await.expect("poll"), before);
}

#[tokio::test]
async fn an_executor_runs_the_task_and_poll_returns_its_real_result() {
    let runner = TokioTaskRunner::with_executor(Arc::new(ValueExecutor(json!({"real": true}))));
    let ticket = runner.start(spec()).await.unwrap();
    let result = settled(&runner, &ticket).await;
    assert_eq!(result, TaskState::Done(json!({"real": true})));
    assert_eq!(runner.poll(&ticket).await.unwrap(), result);
}

#[tokio::test]
async fn executor_errors_settle_as_failures_instead_of_leaving_running_tasks() {
    let runner = TokioTaskRunner::with_executor(Arc::new(FailedExecutor));
    let ticket = runner.start(spec()).await.unwrap();
    assert_eq!(
        settled(&runner, &ticket).await,
        TaskState::Failed("capability error: host action refused".into())
    );
}

#[tokio::test]
async fn executor_panics_settle_as_a_redacted_capability_failure() {
    let runner = TokioTaskRunner::with_executor(Arc::new(PanickingExecutor));
    let ticket = runner.start(spec()).await.unwrap();
    assert_eq!(
        settled(&runner, &ticket).await,
        TaskState::Failed("capability error: task executor panicked".into())
    );
}

#[tokio::test]
async fn cancelling_a_pending_executor_task_keeps_the_terminal_state_stable() {
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let runner = TokioTaskRunner::with_executor(Arc::new(BlockingExecutor {
        started: Arc::clone(&started),
        release: Arc::clone(&release),
    }));
    let ticket = runner.start(spec()).await.unwrap();
    started.notified().await;
    runner.cancel(&ticket).await.unwrap();
    release.notify_one();
    let cancelled = runner.poll(&ticket).await.unwrap();
    assert_eq!(cancelled, TaskState::Failed("cancelled".into()));
    tokio::task::yield_now().await;
    assert_eq!(runner.poll(&ticket).await.unwrap(), cancelled);
}
