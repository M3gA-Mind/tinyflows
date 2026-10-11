use super::*;
use serde_json::json;

#[tokio::test]
async fn bus_dispatch_returns_typed_validation_report() {
    let bus = tinybus::transport::memory::MemoryBus::new();
    tinybus::broker::Broker::new().spawn(bus.clone());
    let server = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    setup(server.clone()).await.unwrap();
    let client = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let proxy = client
        .proxy(BUS_NAME, OBJECT_PATH, tinyflows_bus::INTERFACE)
        .unwrap();
    let report: GraphValidation = proxy
        .call(
            tinyflows_bus::methods::VALIDATE_GRAPH,
            &(json!({"nodes": "invalid"}),),
        )
        .await
        .unwrap();
    assert_eq!(report.errors[0].code, "unparseable_graph");
    assert!(!report.is_valid());
}
