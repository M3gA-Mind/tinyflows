
#[tokio::test]
async fn prune_keeps_a_window_per_namespace() {
    let cp = InMemoryCheckpointer::<i32>::new();
    // An embedded subgraph writes under the parent's thread but its own
    // namespace, interleaved with the parent's records. The two lineages are
    // disjoint — no parent record ever references a child id — so a
    // thread-wide recency window would delete the child lineage outright and
    // leave the thread unresumable.
    let sub = |id: &str, parent: Option<&str>, step: usize| {
        let mut c = checkpoint("t", id, parent, step);
        c.namespace = vec!["sub".to_string()];
        c
    };
    cp.put(checkpoint("t", "p1", None, 1)).await.unwrap();
    cp.put(sub("s1", None, 1)).await.unwrap();
    cp.put(sub("s2", Some("s1"), 2)).await.unwrap();
    cp.put(checkpoint("t", "p2", Some("p1"), 2)).await.unwrap();

    // Keep the last 1 of each namespace plus its ancestors: {p2, p1} and
    // {s2, s1} — nothing is deleted, and the subgraph stays resolvable.
    let removed = cp.prune("t", 1).await.unwrap();
    assert_eq!(removed, 0);
    let child = cp
        .get_scoped("t", None, &["sub".to_string()])
        .await
        .unwrap()
        .expect("the subgraph namespace must stay resumable after prune");
    assert_eq!(child.checkpoint_id, "s2");
}
