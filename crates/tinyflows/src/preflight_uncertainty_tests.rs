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
                    "subject":"=nodes.shape.item.subject", "body":"=nodes.stable.item.missing"
                }}}
            ],
            "edges":[
                {"from_node":"t","to_node":"shape"}, {"from_node":"shape","to_node":"send"},
                {"from_node":"t","to_node":"stable"}, {"from_node":"stable","to_node":"send"}
            ]
        })).unwrap();
        let errors = unresolvable_tool_args(&graph, &[]).await;
        assert_eq!(errors.len(), 1, "{expression}: {errors:?}");
        assert!(errors[0].contains("arg `body`"), "{errors:?}");
    }
}
