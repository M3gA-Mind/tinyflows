//! The portable validation pipeline, independent of host policy and stores.

use serde_json::Value;
use tinyflows_bus::{GraphValidation, ValidationError};

/// Migrates candidate JSON without dropping editor fields and locates validation failures.
pub(crate) fn validate_graph(value: Value) -> Result<GraphValidation, serde_json::Error> {
    let migrated = tinyflows::migrate::migrate(value);
    let graph = match migrated
        .as_ref()
        .map_err(ToString::to_string)
        .and_then(|value| tinyflows::migrate::deserialize_graph(value.clone()))
    {
        Ok(graph) => graph,
        Err(message) => {
            return Ok(GraphValidation {
                graph: None,
                errors: vec![ValidationError {
                    code: "unparseable_graph".into(),
                    message,
                    node_id: None,
                    field: None,
                }],
            });
        }
    };
    let mut errors: Vec<_> = tinyflows::validate::validate_all(&graph)
        .into_iter()
        .map(|error| ValidationError {
            code: error.code().into(),
            message: error.to_string(),
            node_id: error.node_id().map(str::to_owned),
            field: error.input_name().map(str::to_owned),
        })
        .collect();
    if errors.is_empty() {
        errors = tinyflows::compat::errors(&graph)
            .into_iter()
            .map(|error| ValidationError {
                code: error.code.into(),
                message: error.message,
                node_id: error.node_id,
                field: None,
            })
            .collect();
    }
    Ok(GraphValidation {
        graph: migrated.ok(),
        errors,
    })
}

#[cfg(test)]
#[path = "validation_tests.rs"]
mod tests;
