use super::super::{Running, ScheduleService};
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
    service.runtimes.lock().await.insert(
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
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(10), &mut first)
            .await
            .is_err(),
        "the first caller deadline expires while module shutdown remains held"
    );

    let retry_service = service.clone();
    let retry_handle = handle.clone();
    let retry = tokio::spawn(async move { retry_service.stop_scheduler(retry_handle).await });
    let duplicate_service = service.clone();
    let duplicate_handle = handle.clone();
    let duplicate =
        tokio::spawn(async move { duplicate_service.stop_scheduler(duplicate_handle).await });
    let retry_remained_pending =
        tokio::time::timeout(std::time::Duration::from_millis(10), &mut retry)
            .await
            .is_err();
    let duplicate_remained_pending =
        tokio::time::timeout(std::time::Duration::from_millis(10), &mut duplicate)
            .await
            .is_err();

    let _ = release.send(());
    let first_result = first.await.unwrap();
    let retry_result = retry.await.unwrap();
    let duplicate_result = duplicate.await.unwrap();
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
    service.runtimes.lock().await.insert(
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
    let remained_pending = tokio::time::timeout(std::time::Duration::from_millis(10), &mut retry)
        .await
        .is_err();
    let _ = release.send(());
    let retry_result = retry.await.unwrap();
    assert!(remained_pending, "retry must join supervisor-owned cleanup");
    assert!(retry_result.is_ok());
}
