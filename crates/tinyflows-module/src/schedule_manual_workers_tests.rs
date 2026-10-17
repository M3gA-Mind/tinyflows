use super::*;

fn prepare(scope: &str, request_id: &str) -> PrepareManualRun {
    PrepareManualRun {
        scope: scope.into(),
        request_id: request_id.into(),
    }
}

#[test]
fn prepare_is_idempotent_per_scope_and_acknowledged_handles_are_never_reused() {
    let mut workers = ManualWorkers::default();
    let first = workers.prepare(prepare("profile-a", "request-1"));
    let repeated = workers.prepare(prepare("profile-a", "request-1"));
    let other_scope = workers.prepare(prepare("profile-b", "request-1"));
    assert_eq!(first, repeated);
    assert_ne!(first, other_scope);

    let reservation = workers.get(&first).unwrap();
    assert!(matches!(
        reservation.entry.request_stop(),
        ManualRunState::Terminal { .. }
    ));
    {
        let mut control = reservation.entry.control.lock().unwrap();
        control.expires_at = Instant::now() - Duration::from_secs(24 * 60 * 60);
    }
    assert!(workers.get(&first).is_some(), "terminal results persist until Ack");
    assert!(workers.acknowledge(&first));
    assert!(workers.get(&first).is_none());

    let after_ack = workers.prepare(prepare("profile-a", "request-1"));
    assert_ne!(first, after_ack);
    assert!(
        workers.get(&first).is_none(),
        "stale Start cannot recover an acknowledged handle"
    );
}

#[test]
fn duplicate_start_is_idempotent_but_a_changed_binding_is_refused() {
    let mut workers = ManualWorkers::default();
    let handle = workers.prepare(prepare("profile-a", "request-2"));
    let entry = workers.get(&handle).unwrap().entry;
    let binding = serde_json::json!({"callback":"agent-a","store":"store-a"});
    assert!(entry.begin_start(binding.clone()).unwrap());
    assert!(!entry.begin_start(binding).unwrap());
    assert!(
        entry
            .begin_start(serde_json::json!({"callback":"agent-b","store":"store-a"}))
            .is_err()
    );
    assert!(matches!(
        entry.request_stop(),
        ManualRunState::Stopping { .. }
    ));
    assert!(
        !workers.acknowledge(&handle),
        "live workers cannot be acknowledged"
    );
}

#[test]
fn active_worker_state_is_not_expired_by_the_inert_reservation_ttl() {
    let mut workers = ManualWorkers::default();
    let handle = workers.prepare(prepare("profile-a", "request-3"));
    let entry = workers.get(&handle).unwrap().entry;
    entry
        .begin_start(serde_json::json!({"job":"job-a"}))
        .unwrap();
    {
        let mut control = entry.control.lock().unwrap();
        control.expires_at = Instant::now() - Duration::from_secs(1);
    }
    assert!(
        workers.get(&handle).is_some(),
        "a started worker has no TTL"
    );
}

#[test]
fn stop_before_worker_subscription_is_retained_for_the_worker() {
    let mut workers = ManualWorkers::default();
    let handle = workers.prepare(prepare("profile-a", "request-stop-before-subscribe"));
    let entry = workers.get(&handle).unwrap().entry;
    entry
        .begin_start(serde_json::json!({"job":"job-a"}))
        .unwrap();

    entry.request_stop();
    let receiver = entry.stop.subscribe();
    assert!(
        *receiver.borrow(),
        "a stop signal sent before subscribe must persist"
    );
}
