//! The decision lease: exclusion across store instances, release on drop, and
//! recovery from a holder that went away.

use std::sync::mpsc;
use std::time::Duration;

use serde_json::json;
use tinyflows::store::WorkflowStore;
use tinystoragedrivers_core::{MemoryStorage, Precondition};

use super::test_support::{block_on, docs_in, store_over};
use super::{DriverWorkflowStore, LOCKS};

#[test]
fn separate_store_instances_serialize_proposal_decisions() {
    let storage = MemoryStorage::new();
    let first = store_over(&storage, "local");
    let second = store_over(&storage, "local");
    let first_claim = first
        .lock_proposal_decision("race")
        .expect("claim proposal decisions");

    let (sent, received) = mpsc::channel();
    let contender = std::thread::spawn(move || {
        let claim = second.lock_proposal_decision("race");
        sent.send(claim.map(drop)).expect("report decision claim");
    });
    assert!(
        received.recv_timeout(Duration::from_millis(100)).is_err(),
        "another store must wait before deciding the same workflow"
    );

    drop(first_claim);
    received
        .recv_timeout(Duration::from_secs(5))
        .expect("claim completes after release")
        .expect("claim succeeds");
    contender.join().expect("contender thread");
}

#[test]
fn different_workflows_do_not_wait_for_each_other() {
    let storage = MemoryStorage::new();
    let store = store_over(&storage, "local");
    let _one = store.lock_proposal_decision("one").unwrap();
    let _two = store.lock_proposal_decision("two").unwrap();
}

#[test]
fn different_scopes_do_not_wait_for_each_other() {
    let storage = MemoryStorage::new();
    let _alice = store_over(&storage, "alice")
        .lock_proposal_decision("same")
        .unwrap();
    let _bob = store_over(&storage, "bob")
        .lock_proposal_decision("same")
        .unwrap();
}

#[test]
fn a_released_lease_can_be_claimed_again_at_once() {
    let storage = MemoryStorage::new();
    let store = store_over(&storage, "local");
    drop(store.lock_proposal_decision("w").unwrap());
    drop(store.lock_proposal_decision("w").unwrap());
}

#[test]
fn an_expired_lease_is_taken_over() {
    // A holder that crashed leaves its lease behind; once it runs out the next
    // claimant takes it instead of waiting forever.
    let storage = MemoryStorage::new();
    let docs = docs_in(&storage, "local");
    block_on(docs.put(
        LOCKS,
        "orphaned",
        json!({ "workflow_id": "orphaned", "owner": "a-crashed-holder", "expires_at": 1 }),
        Precondition::None,
    ))
    .unwrap();
    let store = DriverWorkflowStore::new(docs);
    let _claim = store.lock_proposal_decision("orphaned").unwrap();
}

#[test]
fn a_lease_without_an_expiry_counts_as_expired() {
    let storage = MemoryStorage::new();
    let docs = docs_in(&storage, "local");
    block_on(docs.put(
        LOCKS,
        "odd",
        json!({ "workflow_id": "odd", "owner": "someone" }),
        Precondition::None,
    ))
    .unwrap();
    let store = DriverWorkflowStore::new(docs);
    let _claim = store.lock_proposal_decision("odd").unwrap();
}

#[test]
fn releasing_never_deletes_a_lease_someone_else_took_over() {
    let storage = MemoryStorage::new();
    let docs = docs_in(&storage, "local");
    let store = DriverWorkflowStore::new(docs.clone());
    let claim = store.lock_proposal_decision("w").unwrap();
    // The lease is taken over (as after an expiry) while the first holder runs on.
    block_on(docs.put(
        LOCKS,
        "w",
        json!({ "workflow_id": "w", "owner": "the-new-holder", "expires_at": i64::MAX }),
        Precondition::None,
    ))
    .unwrap();
    drop(claim);
    let remaining = block_on(docs.get(LOCKS, "w")).unwrap().expect("still held");
    assert_eq!(remaining.doc["owner"], "the-new-holder");
}

#[test]
fn a_lease_id_that_is_not_usable_is_refused() {
    let store = store_over(&MemoryStorage::new(), "local");
    assert!(store.lock_proposal_decision("").is_err());
}
