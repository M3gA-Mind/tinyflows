use super::*;
use crate::{caps::WorkflowResolver, error::Result, model::WorkflowGraph};
use serde_json::json;
use std::sync::Mutex;

struct Host {
    calls: Mutex<Vec<HostGateStage>>,
    refuse: Option<HostGateStage>,
}

#[async_trait::async_trait]
impl WorkflowResolver for Host {
    async fn resolve(&self, _: &str) -> Result<WorkflowGraph> {
        Err(crate::error::EngineError::Capability("missing".into()))
    }
}

#[async_trait::async_trait]
impl BuilderGateHost for Host {
    async fn check(&self, stage: HostGateStage, _: &WorkflowGraph) -> Result<Vec<String>> {
        self.calls.lock().unwrap().push(stage);
        Ok(if self.refuse == Some(stage) {
            vec![format!("refused {stage:?}")]
        } else {
            vec![]
        })
    }
}

fn graph(nodes: serde_json::Value) -> WorkflowGraph {
    serde_json::from_value(json!({"nodes": nodes, "edges":[]})).unwrap()
}

#[tokio::test]
async fn builder_host_gates_stop_at_the_first_failure_in_declared_order() {
    let stages = [
        HostGateStage::ToolPolicy,
        HostGateStage::AgentReferences,
        HostGateStage::Connections,
        HostGateStage::ToolContracts,
    ];
    let graph = graph(json!([{"id":"start", "name":"Start", "kind":"trigger"}]));
    for (index, stage) in stages.iter().enumerate() {
        let host = Host {
            calls: Default::default(),
            refuse: Some(*stage),
        };
        assert_eq!(
            builder_failures(&graph, &host, &[]).await.unwrap(),
            [format!("refused {stage:?}")]
        );
        assert_eq!(*host.calls.lock().unwrap(), stages[..=index]);
    }
}

#[tokio::test]
async fn portable_authoring_failure_avoids_all_host_catalog_calls() {
    let host = Host {
        calls: Default::default(),
        refuse: None,
    };
    let graph =
        graph(json!([{"id":"code", "name":"Code", "kind":"code", "config":{"language":"shell"}}]));
    let expected = failures(&graph);
    assert!(!expected.is_empty());
    assert_eq!(
        builder_failures(&graph, &host, &[]).await.unwrap(),
        expected
    );
    assert!(host.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn successful_host_gates_proceed_to_mock_preflight_and_respect_native_prefixes() {
    let host = Host {
        calls: Default::default(),
        refuse: None,
    };
    let graph = graph(json!([
        {"id":"start", "name":"Start", "kind":"trigger"},
        {"id":"shape", "name":"Shape", "kind":"transform", "config":{"mode":"template", "template":{"actual":"ok"}}},
        {"id":"send", "name":"Send", "kind":"tool_call", "config":{"slug":"host:send", "args":{"text":"=nodes.shape.item.json.missing"}}}
    ]));
    // Wire the trigger through the deterministic producer so preflight can prove absence.
    let mut graph = graph;
    graph.edges = serde_json::from_value(json!([
        {"from_node":"start", "from_port":"main", "to_node":"shape"},
        {"from_node":"shape", "from_port":"main", "to_node":"send"}
    ]))
    .unwrap();
    assert_eq!(builder_failures(&graph, &host, &[]).await.unwrap().len(), 1);
    assert!(
        builder_failures(&graph, &host, &["host:"])
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(host.calls.lock().unwrap().len(), 8);
}

#[tokio::test]
async fn inline_portable_failures_are_checked_before_any_host_stage() {
    for nodes in [
        json!([{"id":"code","name":"code","kind":"code","config":{"language":"shell"}}]),
        json!([{"id":"agent","name":"agent","kind":"agent","config":{"prompt":"=Go and fetch the issues"}}]),
        json!([
            {"id":"fetch","name":"fetch","kind":"agent","config":{"prompt":"Fetch"}},
            {"id":"send","name":"send","kind":"tool_call","config":{"slug":"host:send","args":{"text":"=nodes.fetch.item.title"}}}
        ]),
    ] {
        let child = graph(nodes);
        let expected: Vec<_> = failures(&child)
            .into_iter()
            .map(|message| format!("Inline sub_workflow node 'inline': {message}"))
            .collect();
        assert!(!expected.is_empty());
        let outer = graph(json!([
            {"id":"start","name":"start","kind":"trigger"},
            {"id":"inline","name":"inline","kind":"sub_workflow","config":{"workflow":child}}
        ]));
        let host = Host {
            calls: Default::default(),
            refuse: None,
        };
        assert_eq!(
            builder_failures(&outer, &host, &["host:"]).await.unwrap(),
            expected
        );
        assert!(host.calls.lock().unwrap().is_empty());
    }
}
