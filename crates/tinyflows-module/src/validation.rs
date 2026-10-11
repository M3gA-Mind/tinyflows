//! The portable validation pipeline, independent of host policy and stores.

use serde_json::Value;
use tinyflows_bus::{GraphValidation, ValidationError};

pub(crate) fn validate_graph(value: Value) -> Result<GraphValidation, serde_json::Error> {
    let graph = match tinyflows::migrate::deserialize_graph(value) {
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
            field: None,
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
        graph: Some(serde_json::to_value(graph)?),
        errors,
    })
}

#[cfg(test)]
#[path = "validation_tests.rs"]
mod tests;
