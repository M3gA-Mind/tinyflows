use super::*;
// Saved workflows reached with a larger remaining budget must be revisited.
use super::tests::{graph, trigger};
use serde_json::json;
use std::collections::HashMap;

struct SavedHost(HashMap<String, WorkflowGraph>);

#[async_trait::async_trait]
impl crate::caps::WorkflowResolver for SavedHost {
    async fn resolve(&self, id: &str) -> crate::error::Result<WorkflowGraph> {
        self.0
            .get(id)
            .cloned()
            .ok_or_else(|| crate::error::EngineError::Capability(id.into()))
    }
}

fn saved_child(id: &str) -> WorkflowGraph {
    graph(
        json!([trigger(), {"id":"child","kind":"sub_workflow","name":"Child","config":{"workflow_id":id}}]),
        json!([{"from_node":"t","to_node":"child"}]),
    )
}

#[tokio::test]
async fn shallower_saved_workflow_visits_discover_descendants_with_remaining_budget() {
    let leaf = graph(
        json!([
            trigger(),
            {"id":"shape","kind":"transform","name":"Shape","config":{"set":{"subject":null}}},
            {"id":"send","kind":"tool_call","name":"Send","config":{"slug":"provider:send","args":{"subject":"=nodes.shape.item.subject"}}}
        ]),
        json!([{"from_node":"t","to_node":"shape"},{"from_node":"shape","to_node":"send"}]),
    );
    let shared = saved_child("leaf");
    let shallow = saved_child("shared");
    let deep = graph(
        json!([trigger(), {"id":"inner","kind":"sub_workflow","name":"Inner","config":{"workflow":shallow}}]),
        json!([{"from_node":"t","to_node":"inner"}]),
    );
    // LIFO discovery visits the later-declared deep branch before the shallow one.
    let root = graph(
        json!([
            {"id":"t","kind":"trigger","name":"Start","config":{"max_sub_workflow_depth":3}},
            {"id":"shallow","kind":"sub_workflow","name":"Shallow","config":{"workflow":shallow}},
            {"id":"deep","kind":"sub_workflow","name":"Deep","config":{"workflow":deep}}
        ]),
        json!([{"from_node":"t","to_node":"shallow"},{"from_node":"t","to_node":"deep"}]),
    );
    let host = SavedHost(HashMap::from([
        ("shared".into(), shared),
        ("leaf".into(), leaf),
    ]));
    let errors = unresolvable_tool_args_with_resolver(&root, &[], Some(&host)).await;
    assert!(
        errors.iter().any(|error| error.contains("arg `subject`")),
        "{errors:?}"
    );
}

#[tokio::test]
async fn dynamic_workflow_ids_do_not_resolve_unrelated_literal_catalog_entries() {
    let leaf = graph(
        json!([trigger(), {"id":"shape","kind":"transform","name":"Shape","config":{"set":{"subject":{}}}},
            {"id":"send","kind":"tool_call","name":"Send","config":{"slug":"provider:send","args":{"subject":"=item.subject.missing"}}}]),
        json!([{"from_node":"t","to_node":"shape"},{"from_node":"shape","to_node":"send"}]),
    );
    let host = SavedHost(HashMap::from([("=run.workflow_id".into(), leaf)]));
    let errors = unresolvable_tool_args_with_resolver(&saved_child("=run.workflow_id"), &[], Some(&host)).await;
    assert!(errors.is_empty(), "{errors:?}");
}
