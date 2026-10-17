use super::*;
use serde_json::json;
use tinybus::{Connection, Result};

#[tokio::test]
async fn wiring_warnings_authoring_member_is_dispatchable() -> Result<()> {
    let bus = tinybus::transport::memory::MemoryBus::new();
    tinybus::broker::Broker::new().spawn(bus.clone());
    let server = Connection::connect(bus.connect().await?).await?;
    super::service::setup(server).await?;
    let client = Connection::connect(bus.connect().await?).await?;
    let proxy = client.proxy(
        tinyflows_bus::BUS_NAME,
        tinyflows_bus::OBJECT_PATH,
        tinyflows_bus::INTERFACE,
    )?;

    let request = json!({
        "graph": {"nodes": [], "edges": []},
        "host": {"bus_name": "test.Contracts", "object_path": "/contracts"}
    });
    let warnings: Vec<String> = proxy.call("WiringWarnings", (request,)).await?;
    assert!(warnings.is_empty(), "empty graph warnings: {warnings:?}");
    Ok(())
}
