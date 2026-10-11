//! TinyBus dispatch and stable dynamic module entry points.

use serde_json::Value;
use tinybus::{Connection, Error, Result};
use tinyflows_bus::{BUS_NAME, GraphValidation, OBJECT_PATH};

struct Authoring;

#[tinybus::interface(name = "ai.tinyhumans.tinyflows.Authoring")]
impl Authoring {
    /// Migrate a candidate and report its structural and engine validation errors.
    async fn validate_graph(&self, graph: Value) -> Result<GraphValidation> {
        super::validation::validate_graph(graph).map_err(|error| Error::failed(error.to_string()))
    }

    /// Report bindings that omit a node's required output envelope.
    async fn binding_failures(&self, graph: Value) -> Result<Vec<String>> {
        let graph = tinyflows::migrate::deserialize_graph(graph).map_err(Error::failed)?;
        Ok(tinyflows::gates::binding_failures(&graph))
    }
}

/// Serve the authoring object and claim its bus identity.
async fn setup(connection: Connection) -> Result<()> {
    connection
        .serve_at(OBJECT_PATH.try_into()?, Authoring)
        .await?;
    connection.request_name(BUS_NAME).await?;
    Ok(())
}

// Generated C entry points implement TinyBus ABI v1.
mod exports {
    tinybus_module::module_export! {
        setup = super::setup,
        worker_threads = 2,
        provides = ["ai.tinyhumans.tinyflows.Authoring"],
        methods = ["ValidateGraph", "BindingFailures"],
        signals = [],
        requires = [],
        optional = [],
        lazy = false,
    }
}

#[cfg(test)]
#[path = "service_tests.rs"]
mod tests;
