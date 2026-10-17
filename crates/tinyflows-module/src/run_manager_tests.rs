use super::*;
use tinyflows_bus::catalog::CallbackAddress;

fn storage(owner: &str) -> tinyflows_bus::catalog::CatalogStorage {
    tinyflows_bus::catalog::CatalogStorage {
        callback: CallbackAddress {
            destination: "host.documents".into(),
            object_path: format!("/owner/{owner}"),
        },
    }
}

#[test]
fn registry_keys_include_the_stable_storage_owner_binding() {
    let run_id = format!("same-id-{}", uuid::Uuid::new_v4());
    let alice = key(&storage("alice"), &run_id);
    let bob = key(&storage("bob"), &run_id);
    assert_ne!(alice, bob);

    let alice_run = register(alice).expect("alice claims the id");
    let bob_run = register(bob).expect("bob may use the same opaque run id");
    assert!(!Arc::ptr_eq(&alice_run, &bob_run));
    unregister(&alice_run);
    unregister(&bob_run);
}

#[test]
fn duplicate_registration_keeps_the_original_cancellation_owner() {
    let run_id = format!("duplicate-{}", uuid::Uuid::new_v4());
    let key = key(&storage("alice"), &run_id);
    let first = register(key.clone()).expect("first reservation succeeds");
    let duplicate = register(key).expect_err("duplicate reservation must fail");
    assert!(Arc::ptr_eq(&first, &duplicate));
    assert!(!first.token.is_cancelled());
    first.token.cancel();
    assert!(duplicate.token.is_cancelled());
    unregister(&first);
}
