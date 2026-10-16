use crate::catalog::test_support::{catalog, trigger_graph};
use crate::catalog::{DEFINITIONS, MAX_REVISIONS_PER_FLOW};

#[tokio::test]
async fn retained_history_count_tracks_visible_prior_snapshots() {
    let store = catalog();
    let flow = store
        .create_flow("v1".into(), trigger_graph(), false, true)
        .await
        .unwrap();
    assert_eq!(store.retained_history_count(&flow.id).await.unwrap(), 0);
    store
        .update_flow_graph(
            &flow.id,
            "v2".into(),
            trigger_graph(),
            false,
            None,
            false,
            Some(&flow.updated_at),
        )
        .await
        .unwrap();
    assert_eq!(store.retained_history_count(&flow.id).await.unwrap(), 1);
}

#[tokio::test]
async fn committed_revision_number_advances_only_after_a_graph_update() {
    let store = catalog();
    let flow = store
        .create_flow("v1".into(), trigger_graph(), false, true)
        .await
        .unwrap();
    assert_eq!(
        store.latest_revision_number(&flow.id).await.unwrap(),
        Some(1)
    );
    store.set_enabled(&flow.id, false).await.unwrap();
    store.record_run(&flow.id, "completed").await.unwrap();
    assert_eq!(
        store.latest_revision_number(&flow.id).await.unwrap(),
        Some(1)
    );
    let after_metadata = store.get_flow(&flow.id).await.unwrap().unwrap();

    store
        .update_flow_graph(
            &flow.id,
            "v2".into(),
            trigger_graph(),
            false,
            None,
            false,
            Some(&after_metadata.updated_at),
        )
        .await
        .unwrap();
    assert_eq!(
        store.latest_revision_number(&flow.id).await.unwrap(),
        Some(2)
    );
    assert_eq!(
        store
            .update_flow_graph(
                &flow.id,
                "stale".into(),
                trigger_graph(),
                false,
                None,
                false,
                Some(&flow.updated_at),
            )
            .await
            .unwrap_err()
            .to_string(),
        "flow changed since it was loaded"
    );
    assert_eq!(
        store.latest_revision_number(&flow.id).await.unwrap(),
        Some(2)
    );
}

#[tokio::test]
async fn duplicated_workflows_start_a_new_revision_sequence() {
    let store = catalog();
    let source = store
        .create_flow("source".into(), trigger_graph(), false, true)
        .await
        .unwrap();
    store
        .update_flow_graph(
            &source.id,
            "source v2".into(),
            trigger_graph(),
            false,
            None,
            false,
            Some(&source.updated_at),
        )
        .await
        .unwrap();
    let source = store.get_flow(&source.id).await.unwrap().unwrap();
    assert_eq!(
        store.latest_revision_number(&source.id).await.unwrap(),
        Some(2)
    );

    let duplicate = store
        .insert_duplicate_flow(&source, "copy".into())
        .await
        .unwrap();
    assert_eq!(
        store.latest_revision_number(&duplicate.id).await.unwrap(),
        Some(1)
    );
}

#[tokio::test]
async fn a_legacy_definition_bootstraps_its_sequence_from_visible_history() {
    let store = catalog();
    let flow = store
        .create_flow("v1".into(), trigger_graph(), false, true)
        .await
        .unwrap();
    let updated = store
        .update_flow_graph(
            &flow.id,
            "v2".into(),
            trigger_graph(),
            false,
            None,
            false,
            Some(&flow.updated_at),
        )
        .await
        .unwrap();
    let docs = store.docs().await.unwrap();
    let stored = docs.get(DEFINITIONS, &flow.id).await.unwrap().unwrap();
    let mut legacy = stored.doc.clone();
    legacy.as_object_mut().unwrap().remove("revision");
    docs.put(DEFINITIONS, &flow.id, legacy, stored.unchanged())
        .await
        .unwrap();

    assert_eq!(
        store.latest_revision_number(&flow.id).await.unwrap(),
        Some(2)
    );
    store.set_enabled(&flow.id, false).await.unwrap();
    let metadata_only = docs.get(DEFINITIONS, &flow.id).await.unwrap().unwrap();
    assert!(metadata_only.doc.get("revision").is_none());
    let metadata_only_flow = store.get_flow(&flow.id).await.unwrap().unwrap();
    store
        .update_flow_graph(
            &flow.id,
            "v3".into(),
            trigger_graph(),
            false,
            None,
            false,
            Some(&metadata_only_flow.updated_at),
        )
        .await
        .unwrap();
    assert_eq!(
        store.latest_revision_number(&flow.id).await.unwrap(),
        Some(3)
    );
    assert_ne!(updated.updated_at, metadata_only_flow.updated_at);
}

#[tokio::test]
async fn latest_revision_number_keeps_increasing_after_history_is_pruned() {
    let store = catalog();
    let flow = store
        .create_flow("v1".into(), trigger_graph(), false, true)
        .await
        .unwrap();
    for index in 0..(MAX_REVISIONS_PER_FLOW + 5) {
        let current = store.get_flow(&flow.id).await.unwrap().unwrap();
        store
            .update_flow_graph(
                &flow.id,
                format!("v{}", index + 2),
                trigger_graph(),
                false,
                None,
                false,
                Some(&current.updated_at),
            )
            .await
            .unwrap();
    }
    assert_eq!(
        store.latest_revision_number(&flow.id).await.unwrap(),
        Some((MAX_REVISIONS_PER_FLOW + 6) as u64)
    );
    assert_eq!(
        store.retained_history_count(&flow.id).await.unwrap(),
        MAX_REVISIONS_PER_FLOW as u64
    );
    assert_eq!(
        store
            .list_revisions(&flow.id, MAX_REVISIONS_PER_FLOW)
            .await
            .unwrap()
            .len(),
        MAX_REVISIONS_PER_FLOW
    );
}
