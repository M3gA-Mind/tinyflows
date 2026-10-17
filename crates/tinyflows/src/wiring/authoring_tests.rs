use super::*;
use serde_json::json;

struct Contracts(HashMap<String, ToolContract>);

#[async_trait::async_trait]
impl ToolContractLookup for Contracts {
    async fn lookup(&self, slug: &str) -> Option<ToolContract> {
        self.0.get(slug).cloned()
    }
}

#[derive(Default)]
struct TrackingLookup(std::sync::Mutex<Vec<String>>);

#[async_trait::async_trait]
impl ToolContractLookup for TrackingLookup {
    async fn lookup(&self, slug: &str) -> Option<ToolContract> {
        self.0.lock().unwrap().push(slug.to_owned());
        None
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
                primary_array_path: Some("data.issues".into()),
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
                primary_array_path: Some("payload.items".into()),
                envelope_prefix: Some("payload".into()),
                metadata_fields: vec!["trace".into()],
            },
        ),
        ("provider:opaque".into(), ToolContract::default()),
        (
            "provider:probed".into(),
            ToolContract {
                output_fields: vec!["items".into()],
                primary_array_path: Some("payload.items".into()),
                envelope_prefix: Some("payload".into()),
                ..ToolContract::default()
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
        {"id":"opaque","name":"Opaque","kind":"tool_call","config":{"slug":"provider:opaque"}},
        {"id":"dynamic","name":"Dynamic","kind":"tool_call","config":{"slug":"=nodes.trigger.item.slug"}},
        {"id":"whole","name":"Whole","kind":"transform","config":{"value":"=nodes.known.item.json.data"}},
        {"id":"metadata","name":"Metadata","kind":"transform","config":{"value":"=nodes.known.item.json.request_id"}},
        {"id":"valid","name":"Valid","kind":"transform","config":{"value":"=nodes.known.item.json.data.issues"}},
        {"id":"unknown_field","name":"Unknown field","kind":"transform","config":{"value":"=nodes.unknown.item.json.payload.maybe"}},
        {"id":"opaque_field","name":"Opaque field","kind":"transform","config":{"value":"=nodes.opaque.item.json.payload.maybe"}},
        {"id":"dynamic_field","name":"Dynamic field","kind":"transform","config":{"value":"=nodes.dynamic.item.json.payload.maybe"}}
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

#[tokio::test]
async fn host_probe_primary_array_overrides_an_unknown_schema() {
    let candidate = graph(json!({"nodes":[
        {"id":"tool","name":"Tool","kind":"tool_call","config":{"slug":"provider:probed"}},
        {"id":"split","name":"Split","kind":"split_out","config":{"path":"json.payload"}}
    ],"edges":[{"from_node":"tool","from_port":"main","to_node":"split"}]}));
    let warnings = wiring_warnings(&candidate, &fixture()).await;
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert!(warnings[0].contains("json.payload.items"));
}

#[tokio::test]
async fn dynamic_slugs_do_not_call_the_host_contract_lookup() {
    let candidate = graph(json!({"nodes":[
        {"id":"tool","name":"Tool","kind":"tool_call","config":{"slug":"=nodes.trigger.item.slug"}}
    ],"edges":[]}));
    let lookup = TrackingLookup::default();
    assert!(wiring_warnings(&candidate, &lookup).await.is_empty());
    assert!(lookup.0.lock().unwrap().is_empty());
}

#[tokio::test]
async fn each_static_tool_slug_is_looked_up_once_in_graph_order() {
    let candidate = graph(json!({"nodes":[
        {"id":"first","name":"First","kind":"tool_call","config":{"slug":"provider:first"}},
        {"id":"second","name":"Second","kind":"tool_call","config":{"slug":"provider:second"}},
        {"id":"repeat","name":"Repeat","kind":"tool_call","config":{"slug":"provider:first"}}
    ],"edges":[]}));
    let lookup = TrackingLookup::default();
    assert!(wiring_warnings(&candidate, &lookup).await.is_empty());
    assert_eq!(
        *lookup.0.lock().unwrap(),
        ["provider:first", "provider:second"]
    );
}

#[tokio::test]
async fn schema_paths_normalize_missing_and_empty_prefixes_and_keep_prefix_named_fields() {
    let contracts = Contracts(HashMap::from([
        (
            "provider:unwrapped".into(),
            ToolContract {
                output_schema: Some(
                    json!({"type":"object", "properties":{"data":{"type":"array"}}}),
                ),
                ..ToolContract::default()
            },
        ),
        (
            "provider:empty-prefix".into(),
            ToolContract {
                output_schema: Some(
                    json!({"type":"object", "properties":{"data":{"type":"array"}}}),
                ),
                envelope_prefix: Some(String::new()),
                ..ToolContract::default()
            },
        ),
        (
            "provider:collision".into(),
            ToolContract {
                output_schema: Some(
                    json!({"type":"object", "properties":{"payload":{"type":"object", "properties":{"items":{"type":"array"}}}}}),
                ),
                primary_array_path: Some("payload.items".into()),
                envelope_prefix: Some("payload".into()),
                ..ToolContract::default()
            },
        ),
        (
            "provider:root-array".into(),
            ToolContract {
                output_schema: Some(json!({"type":"array"})),
                primary_array_path: Some(String::new()),
                ..ToolContract::default()
            },
        ),
    ]));
    let candidate = graph(json!({"nodes":[
        {"id":"unwrapped","name":"Unwrapped","kind":"tool_call","config":{"slug":"provider:unwrapped"}},
        {"id":"empty","name":"Empty prefix","kind":"tool_call","config":{"slug":"provider:empty-prefix"}},
        {"id":"collision","name":"Collision","kind":"tool_call","config":{"slug":"provider:collision"}},
        {"id":"root","name":"Root array","kind":"tool_call","config":{"slug":"provider:root-array"}},
        {"id":"unwrapped_split","name":"Unwrapped split","kind":"split_out","config":{"path":"json.data"}},
        {"id":"empty_split","name":"Empty split","kind":"split_out","config":{"path":"json.data"}},
        {"id":"collision_split","name":"Collision split","kind":"split_out","config":{"path":"json.payload.items"}},
        {"id":"root_split","name":"Root split","kind":"split_out","config":{"path":"json"}}
    ],"edges":[
        {"from_node":"unwrapped","from_port":"main","to_node":"unwrapped_split"},
        {"from_node":"empty","from_port":"main","to_node":"empty_split"},
        {"from_node":"collision","from_port":"main","to_node":"collision_split"},
        {"from_node":"root","from_port":"main","to_node":"root_split"}
    ]}));
    assert!(wiring_warnings(&candidate, &contracts).await.is_empty());
}

#[test]
fn public_data_envelope_helper_delegates_to_generic_schema_resolution() {
    let schema = json!({"type":"object", "properties":{"data":{"type":"array"}}});
    assert_eq!(
        crate::wiring::schema_says_path_is_non_array(&schema, "json.data"),
        Some(true)
    );
    assert_eq!(
        schema_says_path_is_non_array(&schema, "json.data", None),
        Some(false)
    );
    assert_eq!(
        schema_says_path_is_non_array(&schema, "json.data", Some("")),
        Some(false)
    );
}
