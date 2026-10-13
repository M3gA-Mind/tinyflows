use super::*;
use crate::test_support::{docs, expected, fixtures, tree_digest};
use serde_json::json;

fn target() -> FlowCatalogDocuments {
    FlowCatalogDocuments::new(docs())
}

fn keys() -> (String, Vec<String>) {
    let expected = expected();
    let state = &expected["state"];
    (
        state["namespace"].as_str().unwrap().to_string(),
        state["keys"]
            .as_array()
            .unwrap()
            .iter()
            .map(|key| key.as_str().unwrap().to_string())
            .collect(),
    )
}

#[tokio::test]
async fn the_named_keys_come_across_under_their_namespace() {
    let (namespace, keys) = keys();
    let catalog = target();
    let report = import_file_state(&fixtures().join("file-state"), &namespace, &keys, &catalog)
        .await
        .unwrap();
    assert!(report.is_clean());
    assert_eq!(report.tally("state").written, 2, "`never-written` has no file");
    assert_eq!(
        catalog.kv_get(&namespace, "cursor").await.unwrap(),
        Some(json!({ "page": 9 }))
    );
    assert_eq!(
        catalog.kv_get(&namespace, "weird/key with spaces").await.unwrap(),
        Some(json!([1, 2, 3]))
    );
    assert_eq!(catalog.kv_get(&namespace, "never-written").await.unwrap(), None);
}

#[tokio::test]
async fn a_key_in_the_wrong_namespace_finds_nothing() {
    let (_, keys) = keys();
    let report = import_file_state(&fixtures().join("file-state"), "workflow:other", &keys, &target())
        .await
        .unwrap();
    assert_eq!(report.total_written(), 0);
}

#[tokio::test]
async fn importing_again_keeps_what_the_state_store_holds() {
    let (namespace, keys) = keys();
    let catalog = target();
    catalog.kv_set(&namespace, "cursor", &json!("newer")).await.unwrap();
    let report = import_file_state(&fixtures().join("file-state"), &namespace, &keys, &catalog)
        .await
        .unwrap();
    assert_eq!(report.tally("state").existing, 1);
    assert_eq!(report.tally("state").written, 1);
    assert_eq!(catalog.kv_get(&namespace, "cursor").await.unwrap(), Some(json!("newer")));
}

#[tokio::test]
async fn the_legacy_files_are_left_exactly_as_they_were() {
    let (namespace, keys) = keys();
    let before = tree_digest(&fixtures());
    import_file_state(&fixtures().join("file-state"), &namespace, &keys, &target())
        .await
        .unwrap();
    assert_eq!(tree_digest(&fixtures()), before);
}

#[tokio::test]
async fn a_state_file_that_is_not_json_is_counted_not_fatal() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join(digest("ns"));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join(format!("{}.json", digest("bad"))), "{oops").unwrap();
    std::fs::write(dir.join(format!("{}.json", digest("good"))), "42").unwrap();
    let catalog = target();
    let report = import_file_state(root.path(), "ns", &["bad".into(), "good".into()], &catalog)
        .await
        .unwrap();
    assert_eq!(report.tally("state").invalid, 1);
    assert_eq!(report.tally("state").written, 1);
    assert_eq!(catalog.kv_get("ns", "good").await.unwrap(), Some(json!(42)));
}

#[tokio::test]
async fn a_key_whose_file_cannot_be_read_is_an_error() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join(digest("ns"));
    std::fs::create_dir_all(dir.join(format!("{}.json", digest("k")))).unwrap();
    let outcome = import_file_state(root.path(), "ns", &["k".into()], &target()).await;
    assert!(outcome.is_err());
}

#[tokio::test]
async fn no_keys_and_no_directory_are_empty_reports() {
    let root = tempfile::tempdir().unwrap();
    let report = import_file_state(root.path(), "ns", &[], &target()).await.unwrap();
    assert_eq!(report.total_written(), 0);
    let report = import_file_state(&root.path().join("none"), "ns", &["k".into()], &target())
        .await
        .unwrap();
    assert_eq!(report.total_written(), 0);
}

#[test]
fn the_digest_is_the_hex_sha256_the_old_store_used() {
    assert_eq!(
        digest("abc"),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
}
