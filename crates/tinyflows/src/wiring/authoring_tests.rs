use super::*;
use serde_json::json;

struct Contracts(HashMap<String, ToolContract>);

#[async_trait::async_trait]
impl ToolContractLookup for Contracts {
    async fn lookup(&self, slug: &str) -> Option<ToolContract> {
        self.0.get(slug).cloned()
    }
}

fn graph(value: Value) -> WorkflowGraph {
    crate::migrate::deserialize_graph(value).unwrap()
}

fn fixture() -> Contracts {
    Contracts(HashMap::from([
        (
            "provider:issues".into(),
            ToolContract {
                output_fields: vec!["issues".into()],
                output_schema: Some(
                    json!({"type":"object", "properties":{"issues":{"type":"array"},"count":{"type":"integer"}}}),
                ),
                primary_array_path: Some("issues".into()),
                envelope_prefix: Some("data".into()),
                metadata_fields: vec!["request_id".into()],
            },
        ),
        (
            "other:items".into(),
            ToolContract {
                output_fields: vec!["items".into()],
                output_schema: Some(
                    json!({"type":"object", "properties":{"items":{"type":"array"}}}),
                ),
                primary_array_path: Some("items".into()),
                envelope_prefix: Some("payload".into()),
                metadata_fields: vec!["trace".into()],
            },
        ),
    ]))
}

#[tokio::test]
async fn checks_missing_envelopes_fields_and_split_paths_for_multiple_providers() {
    let candidate = graph(json!({"nodes":[
        {"id":"first","name":"First","kind":"tool_call","config":{"slug":"provider:issues"}},
        {"id":"second","name":"Second","kind":"tool_call","config":{"slug":"other:items"}},
        {"id":"bad_field","name":"Bad field","kind":"transform","config":{"value":"=nodes.first.item.json.data.nope"}},
        {"id":"missing_wrapper","name":"Missing wrapper","kind":"transform","config":{"value":"=nodes.second.item.json.items"}},
        {"id":"split","name":"Split","kind":"split_out","config":{"path":"json.data.count"}}
    ],"edges":[{"from_node":"first","from_port":"main","to_node":"split"}]}));
    let warnings = wiring_warnings(&candidate, &fixture()).await;
    assert_eq!(warnings.len(), 3, "{warnings:?}");
    assert!(
        warnings
            .iter()
            .any(|warning| warning.contains("field `nope`"))
    );
    assert!(warnings.iter().any(|warning| warning.contains("payload")));
    assert!(
        warnings
            .iter()
            .any(|warning| warning.contains("json.data.issues"))
    );
}

#[tokio::test]
async fn whole_payload_metadata_valid_fields_and_unknown_contracts_stay_silent() {
    let candidate = graph(json!({"nodes":[
        {"id":"known","name":"Known","kind":"tool_call","config":{"slug":"provider:issues"}},
        {"id":"unknown","name":"Unknown","kind":"tool_call","config":{"slug":"provider:unknown"}},
        {"id":"whole","name":"Whole","kind":"transform","config":{"value":"=nodes.known.item.json.data"}},
        {"id":"metadata","name":"Metadata","kind":"transform","config":{"value":"=nodes.known.item.json.request_id"}},
        {"id":"valid","name":"Valid","kind":"transform","config":{"value":"=nodes.known.item.json.data.issues"}},
        {"id":"unknown_field","name":"Unknown field","kind":"transform","config":{"value":"=nodes.unknown.item.json.payload.maybe"}}
    ],"edges":[]}));
    let warnings = wiring_warnings(&candidate, &fixture()).await;
    assert!(warnings.is_empty(), "{warnings:?}");
}

#[tokio::test]
async fn schema_without_primary_array_warns_only_when_path_is_known_non_array() {
    let contracts = Contracts(HashMap::from([(
        "provider:schema".into(),
        ToolContract {
            output_fields: vec![],
            output_schema: Some(
                json!({"type":"object", "properties":{"details":{"type":"object"}}}),
            ),
            primary_array_path: None,
            envelope_prefix: Some("payload".into()),
            metadata_fields: vec![],
        },
    )]));
    let candidate = graph(json!({"nodes":[
        {"id":"tool","name":"Tool","kind":"tool_call","config":{"slug":"provider:schema"}},
        {"id":"bad","name":"Bad split","kind":"split_out","config":{"path":"json.payload.details"}},
        {"id":"unknown","name":"Unknown split","kind":"split_out","config":{"path":"json.payload.missing"}}
    ],"edges":[
        {"from_node":"tool","from_port":"main","to_node":"bad"},
        {"from_node":"tool","from_port":"main","to_node":"unknown"}
    ]}));
    let warnings = wiring_warnings(&candidate, &contracts).await;
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert!(warnings[0].contains("known non-array"));
}
