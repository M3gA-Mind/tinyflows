use super::Running;
use tokio::{sync::watch, task::AbortHandle};

pub(super) struct StopCompletionGuard {
    completion: watch::Sender<Option<std::result::Result<(), String>>>,
    running_task: Option<AbortHandle>,
    completed: bool,
}

impl StopCompletionGuard {
    pub(super) fn new(
        completion: watch::Sender<Option<std::result::Result<(), String>>>,
        running_task: Option<AbortHandle>,
    ) -> Self {
        Self {
            completion,
            running_task,
            completed: false,
        }
    }

    pub(super) fn finish(&mut self, result: std::result::Result<(), String>) {
        if result.is_err() {
            if let Some(task) = self.running_task.take() {
                task.abort();
            }
        } else {
            self.running_task.take();
        }
        self.completion.send_replace(Some(result));
        self.completed = true;
    }
}

impl Drop for StopCompletionGuard {
    fn drop(&mut self) {
        if !self.completed {
            if let Some(task) = self.running_task.take() {
                task.abort();
            }
            self.completion.send_replace(Some(Err(
                "scheduler stop supervisor ended without confirming quiescence".into(),
            )));
        }
    }
}

pub(super) fn abort_handle(running: &Running) -> Option<AbortHandle> {
    match running {
        Running::Loop { task, .. } => Some(task.abort_handle()),
        Running::Manual(_) | Running::Stopping(_) => None,
    }
}

pub(super) async fn shutdown_scheduler(running: Running) -> std::result::Result<(), String> {
    match running {
        Running::Loop {
            mut task,
            mut shutdown,
        } => {
            if let Some(shutdown) = shutdown.take() {
                let _ = shutdown.send(());
            }
            (&mut task)
                .await
                .map_err(|error| format!("scheduler shutdown task failed: {error}"))
        }
        Running::Manual(runtime) => {
            runtime.lock().await.shutdown().await;
            Ok(())
        }
        Running::Stopping(_) => Err("scheduler stop was already in progress".into()),
    }
}

pub(super) async fn service_dropped(mut lifetime: watch::Receiver<bool>) {
    while !*lifetime.borrow_and_update() {
        if lifetime.changed().await.is_err() {
            return;
        }
    }
}
