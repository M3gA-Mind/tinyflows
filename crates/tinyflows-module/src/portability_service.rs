//! Compiled-module entry points for host-neutral preparation helpers.

use serde_json::Value;
use tinybus::{Connection, Error, Result};

struct Portability;

#[tinybus::interface(name = "ai.tinyhumans.tinyflows.Portability")]
impl Portability {
    /// Merge a host-resolved credential into a request, failing closed on bad
    /// request/header shapes. The credential value is never included in errors.
    async fn prepare_http_request(
        &self,
        request: tinyflows_bus::portability::PrepareHttpRequest,
    ) -> Result<Value> {
        let credential = tinyflows::caps::host::HttpCredential {
            header: request.header,
            value: request.value,
        };
        tinyflows::caps::host::inject_credential(request.request, &credential)
            .map_err(|_| Error::failed("HTTP request preparation failed"))
    }

    /// Match a graph's sole app-event trigger.
    async fn app_event_binding(
        &self,
        request: tinyflows_bus::portability::AppEventBinding,
    ) -> Result<bool> {
        let graph = tinyflows::migrate::deserialize_graph(request.graph)
            .map_err(|_| Error::failed("invalid workflow graph"))?;
        Ok(tinyflows::triggers::app_event_binding(
            &graph,
            &request.toolkit,
            &request.trigger_slug,
        ))
    }

    /// Return literal inputs pinned on a graph's sole trigger.
    async fn pinned_inputs(
        &self,
        request: tinyflows_bus::portability::PinnedInputs,
    ) -> Result<Value> {
        let graph = tinyflows::migrate::deserialize_graph(request.graph)
            .map_err(|_| Error::failed("invalid workflow graph"))?;
        serde_json::to_value(tinyflows::triggers::pinned_inputs(&graph))
            .map_err(|_| Error::failed("pinned trigger inputs could not be encoded"))
    }
}

pub(super) async fn setup(connection: &Connection) -> Result<()> {
    connection
        .serve_at(
            tinyflows_bus::OBJECT_PATH.try_into()?,
            Portability,
        )
        .await
}
