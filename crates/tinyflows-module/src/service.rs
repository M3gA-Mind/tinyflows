//! TinyBus dispatch and stable dynamic module entry points.

use serde_json::Value;
use tinybus::{Connection, Error, Result};
use tinyflows_bus::{BUS_NAME, GraphValidation, OBJECT_PATH};

struct Authoring {
    connection: Connection,
}

struct Copilot {
    connection: Connection,
}

#[tinybus::interface(name = "ai.tinyhumans.tinyflows.Copilot")]
impl Copilot {
    async fn invoke_tool(
        &self,
        request: tinyflows_bus::copilot::InvokeToolRequest,
    ) -> Result<Value> {
        super::copilot_tool_belt::invoke_tool(self.connection.clone(), request).await
    }

    async fn tool_catalog(
        &self,
        request: tinyflows_bus::copilot::ToolCatalogRequest,
    ) -> Result<Vec<tinyflows_bus::copilot::ToolDescriptor>> {
        let host = std::sync::Arc::new(super::copilot_tools::ScopedCopilotHost {
            connection: self.connection.clone(),
            host: tinyflows_bus::copilot::CopilotHost {
                bus_name: String::new(),
                object_path: String::new(),
            },
        });
        let kinds = std::sync::Arc::new(super::copilot_tool_belt::ScopedNodeKinds::from_wire(
            request.node_kinds,
        )?);
        let tools = super::copilot_tool_belt::tool_belt(host, kinds);
        Ok(tools
            .into_iter()
            .map(|tool| tinyflows_bus::copilot::ToolDescriptor {
                name: tool.name().into(),
                description: tool.description().into(),
                parameters_schema: tool.parameters_schema(),
                permission: format!("{:?}", tool.permission_level()),
                external_effect: tool.external_effect(),
            })
            .collect())
    }

    async fn render_builder_prompt(
        &self,
        request: tinyflows_bus::copilot::BuilderPromptRequest,
    ) -> Result<tinyflows_bus::copilot::TextResponse> {
        let request: tinyflows_copilot::BuilderRequest =
            serde_json::from_value(serde_json::to_value(request).map_err(Error::failed)?)
                .map_err(Error::failed)?;
        request.validate().map_err(Error::failed)?;
        Ok(tinyflows_bus::copilot::TextResponse {
            text: tinyflows_copilot::render_prompt(&request),
        })
    }

    async fn get_prompt(&self, name: String) -> Result<tinyflows_bus::copilot::TextResponse> {
        let text = match name.as_str() {
            "workflow_builder" => tinyflows_copilot::prompts::WORKFLOW_BUILDER,
            "flow_discovery" => tinyflows_copilot::prompts::FLOW_DISCOVERY,
            _ => return Err(Error::failed(format!("unknown copilot prompt '{name}'"))),
        };
        Ok(tinyflows_bus::copilot::TextResponse { text: text.into() })
    }

    async fn get_resource(&self, path: String) -> Result<tinyflows_bus::copilot::TextResponse> {
        let file = tinyflows_copilot::resources::FLOW_AUTHORING_FILES
            .iter()
            .find(|file| file.path == path)
            .ok_or_else(|| Error::failed(format!("unknown copilot resource '{path}'")))?;
        Ok(tinyflows_bus::copilot::TextResponse {
            text: file.contents.into(),
        })
    }

    async fn list_resources(&self) -> Result<Vec<tinyflows_bus::copilot::ResourceFile>> {
        Ok(tinyflows_copilot::resources::FLOW_AUTHORING_FILES
            .iter()
            .map(|file| tinyflows_bus::copilot::ResourceFile {
                path: file.path.into(),
                contents: file.contents.into(),
            })
            .collect())
    }

    async fn build_trail_off_fallback(
        &self,
        request: tinyflows_bus::copilot::TrailOffRequest,
    ) -> Result<tinyflows_bus::copilot::TextResponse> {
        let history = request
            .history
            .into_iter()
            .map(|result| tinyflows_copilot::fallback::ToolResult {
                tool_name: result.tool_name,
                content: result.content,
            })
            .collect::<Vec<_>>();
        Ok(tinyflows_bus::copilot::TextResponse {
            text: tinyflows_copilot::fallback::build_trail_off_fallback(&history),
        })
    }

    async fn combine_trail_off_fallback(
        &self,
        fallback: String,
        original: String,
    ) -> Result<tinyflows_bus::copilot::TextResponse> {
        Ok(tinyflows_bus::copilot::TextResponse {
            text: tinyflows_copilot::fallback::combine_trail_off_fallback(&fallback, &original),
        })
    }

    async fn is_backend_or_infrastructure_failure(&self, error: String) -> Result<bool> {
        Ok(tinyflows_copilot::fallback::is_backend_or_infrastructure_failure(&error))
    }

    async fn backend_repair_message(&self, error: String) -> Result<Option<String>> {
        Ok(tinyflows_copilot::fallback::backend_repair_message(&error))
    }

    async fn text_looks_like_question(&self, text: String) -> Result<bool> {
        Ok(tinyflows_copilot::text_looks_like_question(&text))
    }
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
        let mut warnings = warnings;
        warnings.extend(report.warnings.iter().cloned());
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

    /// Report best-effort generic output-field, envelope, and split path warnings.
    async fn wiring_warnings(
        &self,
        request: tinyflows_bus::WiringWarningsRequest,
    ) -> Result<Vec<String>> {
        let graph = tinyflows::migrate::deserialize_graph(request.graph).map_err(Error::failed)?;
        let ports = super::host::HostPorts {
            connection: self.connection.clone(),
            host: request.host,
        };
        Ok(tinyflows::wiring::wiring_warnings(&graph, &ports).await)
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
    connection
        .serve_at(
            OBJECT_PATH.try_into()?,
            Copilot {
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
    super::portability_service::setup(&connection).await?;
    Ok(())
}

#[cfg(test)]
#[path = "service_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "wiring_authoring_tests.rs"]
mod wiring_authoring_tests;
