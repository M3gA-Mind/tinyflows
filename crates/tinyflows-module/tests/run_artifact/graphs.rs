use super::Owner;
use serde_json::{Value, json};
use tinyflows_bus::runs::{self, FlowRunTrigger, RunContext, StartRun};

pub(crate) fn dedup_graph(node_id: &str) -> Value {
    json!({
        "inputs":[{"name":"declared","type":"string","required":true}],
        "nodes":[
            {"id":"trigger","name":"Trigger","kind":"trigger","config":{}},
            {"id":node_id,"name":"Dedup","kind":"dedup","config":{"key":"=item.id"}},
            {"id":"agent","name":"Agent","kind":"agent","config":{"agent_ref":"test-agent","prompt":"work"}},
            {"id":"output","name":"Output","kind":"transform","config":{"set":{"declared":"=inputs.declared","payload":"=run.trigger.payload"}}}
        ],
        "edges":[
            {"from_node":"trigger","from_port":"main","to_node":node_id,"to_port":"main"},
            {"from_node":node_id,"from_port":"main","to_node":"agent","to_port":"main"},
            {"from_node":"agent","from_port":"main","to_node":"output","to_port":"main"}
        ]
    })
}

pub(crate) fn parked_graph() -> Value {
    json!({
        "inputs":[{"name":"declared","type":"string","required":true}],
        "nodes":[
            {"id":"trigger","name":"Trigger","kind":"trigger","config":{}},
            {"id":"dedup","name":"Dedup","kind":"dedup","config":{"key":"=item.id"}},
            {"id":"approval","name":"Approval","kind":"approval","config":{"request_id":"review-1"}}
        ],
        "edges":[
            {"from_node":"trigger","from_port":"main","to_node":"dedup","to_port":"main"},
            {"from_node":"dedup","from_port":"main","to_node":"approval","to_port":"main"}
        ]
    })
}

pub(crate) fn graph(include_agent: bool) -> Value {
    let mut nodes = vec![json!({"id":"trigger","name":"Trigger","kind":"trigger","config":{}})];
    let mut edges = Vec::new();
    if include_agent {
        nodes.push(json!({"id":"agent","name":"Agent","kind":"agent","config":{"agent_ref":"test-agent","prompt":"work"}}));
        nodes.push(json!({"id":"output","name":"Output","kind":"transform","config":{"set":{"declared":"=inputs.declared","payload":"=run.trigger.payload"}}}));
        edges.push(
            json!({"from_node":"trigger","from_port":"main","to_node":"agent","to_port":"main"}),
        );
        edges.push(
            json!({"from_node":"agent","from_port":"main","to_node":"output","to_port":"main"}),
        );
    }
    json!({
        "inputs":[{"name":"declared","type":"string","required":true}],
        "nodes":nodes,
        "edges":edges
    })
}

pub(crate) fn request(
    owner: &Owner,
    flow_id: String,
    run_id: &str,
    graph: Value,
    timeout_ms: u64,
) -> StartRun {
    let mut inputs = serde_json::Map::new();
    inputs.insert("declared".into(), json!("declared-value"));
    StartRun {
        run_id: run_id.into(),
        context: RunContext {
            flow_id,
            origin: Some(json!({"source":"artifact-test"})),
        },
        caps_host: runs::CapsHost {
            callback: owner.caps.clone(),
        },
        storage: owner.storage.clone(),
        graph,
        trigger_kind: FlowRunTrigger::Rpc,
        trigger_data: json!({"payload":"trigger-value"}),
        inputs,
        timeout_ms,
    }
}
