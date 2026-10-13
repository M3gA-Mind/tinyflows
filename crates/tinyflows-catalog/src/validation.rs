//! Mapping the engine's portable validation findings onto the catalog's
//! structured [`FlowValidationError`], the shape a surface attaches to a node.

use tinyflows::model::WorkflowGraph;

use crate::types::FlowValidationError;

/// Maps a portable [`tinyflows::error::ValidationError`] onto a
/// [`FlowValidationError`], carrying its stable `code`, anchoring `node_id` and
/// human `message`. One place, so the mapping stays consistent across every
/// surface that reports validation.
#[must_use]
pub fn to_flow_validation_error(err: &tinyflows::error::ValidationError) -> FlowValidationError {
    FlowValidationError {
        code: err.code().to_string(),
        message: err.to_string(),
        node_id: err.node_id().map(str::to_string),
        field: None,
    }
}

/// Maps one engine-compatibility refusal onto a [`FlowValidationError`].
#[must_use]
pub fn to_compat_validation_error(
    error: tinyflows::compat::CompatibilityError,
) -> FlowValidationError {
    FlowValidationError {
        code: error.code.to_string(),
        message: error.message,
        node_id: error.node_id,
        field: None,
    }
}

/// Every engine-incompatible topology in `graph`, in the catalog's validation
/// error shape. The classification is [`tinyflows::compat`]'s.
#[must_use]
pub fn engine_compatibility_errors(graph: &WorkflowGraph) -> Vec<FlowValidationError> {
    tinyflows::compat::errors(graph)
        .into_iter()
        .map(to_compat_validation_error)
        .collect()
}

#[cfg(test)]
#[path = "validation_tests.rs"]
mod tests;
