//! Runtime trigger mappings must not become proof of missing provider fields.

use super::*;
use serde_json::json;

#[tokio::test]
async fn trigger_fields_mapped_through_transforms_remain_unverifiable() {
    for expression in ["=item.subject", "=run.subject"] {
        let graph: WorkflowGraph = serde_json::from_value(json!({
            "nodes":[
                {"id":"t","kind":"trigger","name":"Start"},
                {"id":"shape","kind":"transform","name":"Shape","config":{"set":{"subject":expression}}},
                {"id":"stable","kind":"transform","name":"Stable","config":{"set":{"body":"hello"}}},
                {"id":"send","kind":"tool_call","name":"Send","config":{"slug":"provider:send","args":{
                    "subject":"=nodes.shape.item.subject", "body":"=nodes.stable.item.body.missing"
                }}}
            ],
            "edges":[
                {"from_node":"t","to_node":"shape"}, {"from_node":"shape","to_node":"send"},
                {"from_node":"t","to_node":"stable"}, {"from_node":"stable","to_node":"send"}
            ]
        })).unwrap();
        let errors = unresolvable_tool_args(&graph, &[]).await;
        // The tool executes once per incoming item in this fan-in.
        assert_eq!(errors.len(), 2, "{expression}: {errors:?}");
        assert!(
            errors.iter().all(|error| error.contains("arg `body`")),
            "{errors:?}"
        );
    }
}

#[tokio::test]
async fn passthrough_trigger_fields_are_unverifiable_but_overwritten_fields_are_known() {
    let graph: WorkflowGraph = serde_json::from_value(json!({
        "nodes":[
            {"id":"t","kind":"trigger","name":"Start"},
            {"id":"shape","kind":"transform","name":"Shape","config":{"set":{"body":{}}}},
            {"id":"relay","kind":"transform","name":"Relay","config":{"set":{"other":"fixed"}}},
            {"id":"send","kind":"tool_call","name":"Send","config":{"slug":"provider:send","args":{
                "subject":"=item.subject", "body":"=item.body.missing"
            }}}
        ],
        "edges":[{"from_node":"t","to_node":"shape"},{"from_node":"shape","to_node":"relay"},{"from_node":"relay","to_node":"send"}]
    })).unwrap();
    let errors = unresolvable_tool_args(&graph, &[]).await;
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].contains("arg `body`"), "{errors:?}");
}
