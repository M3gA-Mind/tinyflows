//! TinyBus dispatch and stable dynamic module entry points.

use serde_json::Value;
use tinybus::{Connection, Error, Result};
use tinyflows_bus::{BUS_NAME, GraphValidation, OBJECT_PATH};

struct Authoring {
    connection: Connection,
}

#[tinybus::interface(name = "ai.tinyhumans.tinyflows.Authoring")]
impl Authoring {
    /// Migrate a candidate and report its structural and engine validation errors.
    async fn validate_graph(
        &self,
        request: tinyflows_bus::ValidateGraphRequest,
    ) -> Result<GraphValidation> {
        let ports = request.host.map(|host| super::host::HostPorts {
            connection: self.connection.clone(),
            host,
        });
        super::validation::validate_graph(request.graph, ports.as_ref().map(|p| p as _))
            .await
            .map_err(|error| Error::failed(error.to_string()))
    }

    /// Normalize native/n8n input, validate it, and return warnings without persistence.
    async fn import_graph(
        &self,
        request: tinyflows_bus::ImportGraphRequest,
    ) -> Result<tinyflows_bus::ImportGraphResult> {
        let format = request
            .format
            .as_deref()
            .unwrap_or("auto")
            .trim()
            .to_ascii_lowercase();
        let is_n8n = match format.as_str() {
            "auto" | "" => tinyflows_catalog::import::n8n::looks_like_n8n(&request.graph),
            "n8n" => true,
            "native" | "tinyflows" => false,
            other => {
                return Err(Error::failed(format!(
                    "unsupported import format '{other}'"
                )));
            }
        };
        let (graph, warnings) = if is_n8n {
            let imported = tinyflows_catalog::import::n8n::map_n8n_workflow(&request.graph)
                .map_err(Error::failed)?;
            (
                serde_json::to_value(imported.graph).map_err(Error::failed)?,
                imported.warnings,
            )
        } else {
            (request.graph, Vec::new())
        };
        let report = super::validation::validate_graph(graph, None)
            .await
            .map_err(|error| Error::failed(error.to_string()))?;
        Ok(tinyflows_bus::ImportGraphResult {
            graph: report.graph,
            warnings,
            errors: report.errors,
        })
    }

    /// Report the full portable authoring gates, preserving their diagnostic order.
    async fn authoring_failures(&self, graph: Value) -> Result<Vec<String>> {
        let graph = tinyflows::migrate::deserialize_graph(graph).map_err(Error::failed)?;
        Ok(tinyflows::gates::authoring_failures(&graph))
    }

    /// Mock-run outbound arguments, excluding the host's configured native slug prefixes.
    async fn unresolvable_tool_args(
        &self,
        graph: Value,
        native_slug_prefixes: Vec<String>,
    ) -> Result<Vec<String>> {
        let graph = tinyflows::migrate::deserialize_graph(graph).map_err(Error::failed)?;
        let prefixes: Vec<&str> = native_slug_prefixes.iter().map(String::as_str).collect();
        Ok(tinyflows::preflight::unresolvable_tool_args(&graph, &prefixes).await)
    }

    /// Compose portable gates and context-bound host checks in cost order.
    async fn builder_failures(
        &self,
        graph: Value,
        host: tinyflows_bus::AuthoringHost,
        native_slug_prefixes: Vec<String>,
    ) -> Result<Vec<String>> {
        let graph = tinyflows::migrate::deserialize_graph(graph).map_err(Error::failed)?;
        let ports = super::host::HostPorts {
            connection: self.connection.clone(),
            host,
        };
        let prefixes: Vec<&str> = native_slug_prefixes.iter().map(String::as_str).collect();
        tinyflows::gates::builder_failures(&graph, &ports, &prefixes)
            .await
            .map_err(|error| Error::failed(error.to_string()))
    }

    /// Report bindings that omit a node's required output envelope.
    async fn binding_failures(&self, graph: Value) -> Result<Vec<String>> {
        let graph = tinyflows::migrate::deserialize_graph(graph).map_err(Error::failed)?;
        Ok(tinyflows::gates::binding_failures(&graph))
    }
}

/// Serve the authoring object and claim its bus identity.
pub(super) async fn setup(connection: Connection) -> Result<()> {
    connection
        .serve_at(
            OBJECT_PATH.try_into()?,
            Authoring {
                connection: connection.clone(),
            },
        )
        .await?;
    connection.request_name(BUS_NAME).await?;
    super::schedule_service::setup(&connection).await?;
    super::catalog_service::setup(&connection)
        .await
        .map_err(|error| Error::failed(error.to_string()))?;
    super::runs_service::setup(&connection).await?;
    Ok(())
}

#[cfg(test)]
#[path = "service_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "wiring_authoring_tests.rs"]
mod wiring_authoring_tests;
