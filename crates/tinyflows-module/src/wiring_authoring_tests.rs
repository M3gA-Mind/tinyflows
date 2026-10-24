use super::*;
use serde_json::json;
use tinybus::{Connection, Result};

struct ContractCallback;

#[tinybus::interface(name = "ai.tinyhumans.tinyflows.AuthoringHost")]
impl ContractCallback {
    async fn resolve_workflow(&self, _: String) -> Result<Option<serde_json::Value>> {
        Ok(None)
    }

    async fn check_gate(&self, _: String, _: serde_json::Value) -> Result<Vec<String>> {
        Ok(Vec::new())
    }

    async fn lookup_tool_contract(
        &self,
        slug: String,
    ) -> Result<Option<tinyflows_bus::ToolContract>> {
        if slug == "provider:error" {
            return Err(tinybus::Error::failed("lookup unavailable"));
        }
        Ok(
            (slug == "provider:issues").then(|| tinyflows_bus::ToolContract {
                output_fields: vec!["issues".into()],
                output_schema: Some(json!({"type":"object"})),
                primary_array_path: Some("data.issues".into()),
                envelope_prefix: Some("data".into()),
                metadata_fields: vec!["request_id".into()],
            }),
        )
    }
}

#[tokio::test]
async fn wiring_warnings_authoring_member_is_dispatchable() -> Result<()> {
    let bus = tinybus::transport::memory::MemoryBus::new();
    tinybus::broker::Broker::new().spawn(bus.clone());
    let server = Connection::connect(bus.connect().await?).await?;
    setup(server).await?;
    let client = Connection::connect(bus.connect().await?).await?;
    let proxy = client.proxy(
        tinyflows_bus::BUS_NAME,
        tinyflows_bus::OBJECT_PATH,
        tinyflows_bus::INTERFACE,
    )?;
    client
        .serve_at("/contracts".try_into()?, ContractCallback)
        .await?;
    client.request_name("test.Contracts").await?;

    let request = tinyflows_bus::WiringWarningsRequest {
        graph: json!({"nodes":[
            {"id":"tool","name":"Tool","kind":"tool_call","config":{"slug":"provider:issues"}},
            {"id":"next","name":"Next","kind":"transform","config":{"value":"=nodes.tool.item.json.data.missing"}},
            {"id":"error_tool","name":"Error tool","kind":"tool_call","config":{"slug":"provider:error"}},
            {"id":"error_next","name":"Error next","kind":"transform","config":{"value":"=nodes.error_tool.item.json.data.missing"}}
        ],"edges":[]}),
        host: tinyflows_bus::AuthoringHost {
            bus_name: "test.Contracts".into(),
            object_path: "/contracts".into(),
        },
    };
    let warnings: Vec<String> = proxy
        .call(tinyflows_bus::methods::WIRING_WARNINGS, (request,))
        .await?;
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert!(warnings[0].contains("missing"));
    assert!(warnings[0].contains("issues"));
    Ok(())
}
