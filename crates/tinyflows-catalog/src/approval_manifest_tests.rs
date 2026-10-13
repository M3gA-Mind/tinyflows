use super::*;

fn graph() -> WorkflowGraph {
    serde_json::from_value(json!({
        "name": "m",
        "nodes": [
            { "id": "t", "kind": "trigger", "name": "T" },
            { "id": "h", "kind": "http_request", "name": "H", "config": { "url": "https://x.test" } },
            { "id": "c", "kind": "code", "name": "C" },
            { "id": "d", "kind": "tool_call", "name": "D", "config": { "slug": "=nodes.t.item.json.s" } },
            { "id": "n1", "kind": "tool_call", "name": "N1", "config": { "slug": "oh:shell", "args": {"a": 1} } },
            { "id": "n2", "kind": "tool_call", "name": "N2", "config": { "slug": "oh:shell" } },
            { "id": "e", "kind": "tool_call", "name": "E", "config": { "slug": "oh:" } },
            { "id": "g", "kind": "tool_call", "name": "G", "config": { "slug": "GMAIL_SEND" } },
            { "id": "r", "kind": "tool_call", "name": "R", "config": { "slug": "GMAIL_LIST" } },
            { "id": "a", "kind": "agent", "name": "A", "config": { "agent_ref": "x" } },
            { "id": "a2", "kind": "agent", "name": "A2", "config": { "agent_ref": " " } }
        ],
        "edges": []
    }))
    .unwrap()
}

fn class(name: &str, blocked: bool, skip: bool) -> Option<ManifestClass> {
    Some(ManifestClass {
        name: name.into(),
        blocked,
        skip,
    })
}

#[test]
fn candidates_follow_graph_order_and_skip_invalid_nodes() {
    let c = manifest_candidates(&graph(), "oh:");
    let ids: Vec<&str> = c
        .iter()
        .map(|c| match c {
            ManifestCandidate::Http { node_id, .. }
            | ManifestCandidate::Code { node_id }
            | ManifestCandidate::DynamicTool { node_id }
            | ManifestCandidate::NativeTool { node_id, .. }
            | ManifestCandidate::ConnectorTool { node_id, .. }
            | ManifestCandidate::Agent { node_id } => node_id.as_str(),
        })
        .collect();
    assert_eq!(ids, ["h", "c", "d", "n1", "n2", "g", "r", "a"]);
}

#[test]
fn assemble_dedupes_on_tool_name_skips_reads_and_marks_blocked() {
    let c = manifest_candidates(&graph(), "oh:");
    let classes = vec![
        class("Network", false, false),
        class("Write", true, false),
        None,
        class("Write", false, false),
        class("Write", false, false),
        class("Write", false, false),
        class("Read", false, true),
        None,
    ];
    let entries = assemble_manifest(&c, &classes);
    let kinds: Vec<(&str, &str)> = entries
        .iter()
        .map(|e| (e["kind"].as_str().unwrap(), e["node_id"].as_str().unwrap()))
        .collect();
    assert_eq!(
        kinds,
        [
            ("approvable", "h"),
            ("blocked", "c"),
            ("dynamic", "d"),
            ("approvable", "n1"),
            ("approvable", "g"),
            ("agent", "a")
        ]
    );
    assert_eq!(entries[0]["label"], "Call https://x.test");
    assert_eq!(entries[0]["tool_name"], "flows_http_request");
    assert_eq!(entries[0]["class"], "Network");
}

#[test]
fn split_trust_reports_nothing_without_a_gate() {
    let entries = vec![
        json!({"kind": "approvable", "tool_name": "a"}),
        json!({"kind": "blocked", "tool_name": "b"}),
    ];
    assert_eq!(
        split_manifest_trust(&entries, false, &HashSet::new()),
        (vec![], vec![])
    );
    assert_eq!(
        split_manifest_trust(&entries, true, &HashSet::new()),
        (vec!["a".to_string()], vec![])
    );
    let trusted: HashSet<String> = ["a".to_string()].into();
    assert_eq!(
        split_manifest_trust(&entries, true, &trusted),
        (vec![], vec!["a".to_string()])
    );
}
