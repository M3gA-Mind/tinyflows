use super::*;
use tinybus::{Connection, broker::Broker, transport::memory::MemoryBus};
use tokio::sync::oneshot;

#[tokio::test(start_paused = true)]
async fn stop_retries_join_inflight_shutdown_after_caller_deadline() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let connection = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let service = std::sync::Arc::new(ScheduleService::new(connection));
    let handle = "scheduler-stop-retry".to_string();
    let (shutdown, stopped) = oneshot::channel();
    let (entered, entered_rx) = oneshot::channel();
    let (release, release_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        let _ = stopped.await;
        let _ = entered.send(());
        let _ = release_rx.await;
    });
    service.runtimes.lock().unwrap().insert(
        handle.clone(),
        Running::Loop {
            task,
            shutdown: Some(shutdown),
        },
    );

    let first_service = service.clone();
    let first_handle = handle.clone();
    let mut first = tokio::spawn(async move { first_service.stop_scheduler(first_handle).await });
    entered_rx.await.unwrap();
    let first_deadline =
        tokio::time::timeout(std::time::Duration::from_millis(10), &mut first).await;
    assert!(
        first_deadline.is_err(),
        "the first caller deadline expires while module shutdown remains held"
    );

    let retry_service = service.clone();
    let retry_handle = handle.clone();
    let mut retry = tokio::spawn(async move { retry_service.stop_scheduler(retry_handle).await });
    let duplicate_service = service.clone();
    let duplicate_handle = handle.clone();
    let mut duplicate =
        tokio::spawn(async move { duplicate_service.stop_scheduler(duplicate_handle).await });
    let retry_deadline =
        tokio::time::timeout(std::time::Duration::from_millis(10), &mut retry).await;
    let retry_remained_pending = retry_deadline.is_err();
    let duplicate_deadline =
        tokio::time::timeout(std::time::Duration::from_millis(10), &mut duplicate).await;
    let duplicate_remained_pending = duplicate_deadline.is_err();

    let _ = release.send(());
    let first_result = match first_deadline {
        Ok(result) => result,
        Err(_) => first.await,
    }
    .unwrap();
    let retry_result = match retry_deadline {
        Ok(result) => result,
        Err(_) => retry.await,
    }
    .unwrap();
    let duplicate_result = match duplicate_deadline {
        Ok(result) => result,
        Err(_) => duplicate.await,
    }
    .unwrap();
    assert!(first_result.is_ok());
    assert!(retry_remained_pending, "retry must join the in-flight stop");
    assert!(
        duplicate_remained_pending,
        "concurrent Stop must join the same shutdown"
    );
    assert!(
        retry_result.is_ok(),
        "a timed-out Stop can be retried safely"
    );
    assert!(duplicate_result.is_ok());
    assert!(service.stop_scheduler(handle).await.is_ok());
    assert!(
        service.stop_scheduler("never-created".into()).await.is_ok(),
        "Stop on an absent handle is an idempotent no-op"
    );
}

#[tokio::test(start_paused = true)]
async fn stop_cleanup_supervisor_survives_leader_handler_cancellation() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let connection = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let service = std::sync::Arc::new(ScheduleService::new(connection));
    let handle = "scheduler-stop-abort".to_string();
    let (shutdown, stopped) = oneshot::channel();
    let (entered, entered_rx) = oneshot::channel();
    let (release, release_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        let _ = stopped.await;
        let _ = entered.send(());
        let _ = release_rx.await;
    });
    service.runtimes.lock().unwrap().insert(
        handle.clone(),
        Running::Loop {
            task,
            shutdown: Some(shutdown),
        },
    );

    let first_service = service.clone();
    let first_handle = handle.clone();
    let first = tokio::spawn(async move { first_service.stop_scheduler(first_handle).await });
    entered_rx.await.unwrap();
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());

    let retry_service = service.clone();
    let retry_handle = handle.clone();
    let mut retry = tokio::spawn(async move { retry_service.stop_scheduler(retry_handle).await });
    let deadline = tokio::time::timeout(std::time::Duration::from_millis(10), &mut retry).await;
    let remained_pending = deadline.is_err();
    let _ = release.send(());
    let retry_result = match deadline {
        Ok(result) => result,
        Err(_) => retry.await,
    }
    .unwrap();
    assert!(remained_pending, "retry must join supervisor-owned cleanup");
    assert!(retry_result.is_ok());
}

#[tokio::test]
async fn stop_cleanup_join_failure_is_retained_and_never_reports_quiescence() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let connection = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let service = ScheduleService::new(connection);
    let handle = "scheduler-stop-failed".to_string();
    let (shutdown, stopped) = oneshot::channel();
    drop(stopped);
    let task = tokio::spawn(std::future::pending::<()>());
    task.abort();
    service.runtimes.lock().unwrap().insert(
        handle.clone(),
        Running::Loop {
            task,
            shutdown: Some(shutdown),
        },
    );

    assert!(service.stop_scheduler(handle.clone()).await.is_err());
    assert!(service.tick_scheduler(handle.clone()).await.is_err());
    assert!(service.drain_scheduler(handle.clone()).await.is_err());
    assert!(matches!(
        service.runtimes.lock().unwrap().get(&handle),
        Some(Running::Stopping(_))
    ));
    assert!(
        service.stop_scheduler(handle).await.is_err(),
        "a cleanup failure remains observable on retry"
    );
}

#[tokio::test(start_paused = true)]
async fn service_drop_cancels_and_reports_an_inflight_stop_supervisor() {
    struct TaskDropSignal(Option<oneshot::Sender<()>>);
    impl Drop for TaskDropSignal {
        fn drop(&mut self) {
            if let Some(signal) = self.0.take() {
                let _ = signal.send(());
            }
        }
    }

    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let connection = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let service = ScheduleService::new(connection);
    let registry = service.runtimes.clone();
    let handle = "scheduler-stop-service-drop".to_string();
    let (shutdown, stopped) = oneshot::channel();
    let (entered, entered_rx) = oneshot::channel();
    let (release, release_rx) = oneshot::channel();
    let (task_dropped, task_dropped_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        let _drop_signal = TaskDropSignal(Some(task_dropped));
        let _ = stopped.await;
        let _ = entered.send(());
        let _ = release_rx.await;
    });
    registry.lock().unwrap().insert(
        handle.clone(),
        Running::Loop {
            task,
            shutdown: Some(shutdown),
        },
    );

    let stopping_service = service;
    let stopping_handle = handle.clone();
    let stop_task =
        tokio::spawn(async move { stopping_service.stop_scheduler(stopping_handle).await });
    tokio::time::timeout(std::time::Duration::from_secs(10), entered_rx)
        .await
        .expect("scheduler cleanup must start before the failure cap")
        .unwrap();
    let mut completion = match registry.lock().unwrap().get(&handle) {
        Some(Running::Stopping(completion)) => completion.subscribe(),
        _ => panic!("stop completion must be registered before shutdown awaits"),
    };

    stop_task.abort();
    assert!(stop_task.await.unwrap_err().is_cancelled());
    let completion_signal =
        tokio::time::timeout(std::time::Duration::from_millis(10), completion.changed()).await;
    let completion_result = completion.borrow().clone();
    let _ = release.send(());
    let task_was_dropped =
        tokio::time::timeout(std::time::Duration::from_secs(10), task_dropped_rx)
            .await
            .is_ok();

    assert!(
        completion_signal.is_ok(),
        "service Drop must stop its independent shutdown supervisor"
    );
    assert!(matches!(completion_result, Some(Err(_))));
    assert!(
        task_was_dropped,
        "service teardown must end the owned loop task"
    );
}
