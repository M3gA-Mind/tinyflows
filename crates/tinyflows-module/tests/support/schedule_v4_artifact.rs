use super::{
    callbacks, schedule_v4_delivery_artifact, schedule_v4_manual_artifact,
    schedule_v4_recovery_artifact,
};
use serde_json::Value;
use std::sync::Arc;
use tinybus::Proxy;
use tinyflows_bus::schedule::CallbackAddress;
use tinystoragedrivers_core::DocumentStore;

pub async fn verify(
    proxy: &Proxy,
    docs: Arc<dyn DocumentStore>,
    storage: Value,
    state: Arc<callbacks::State>,
) {
    let callback = CallbackAddress {
        destination: "test.ScheduleCallbacks".into(),
        object_path: "/test/ScheduleCallbacks".into(),
    };
    schedule_v4_manual_artifact::verify(
        proxy,
        docs.clone(),
        storage.clone(),
        callback.clone(),
        state.clone(),
    )
    .await;
    schedule_v4_recovery_artifact::verify(
        proxy,
        docs.clone(),
        storage.clone(),
        callback.clone(),
        state.clone(),
    )
    .await;
    schedule_v4_delivery_artifact::verify(proxy, docs, storage, callback, state).await;
}
