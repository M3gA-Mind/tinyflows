//! Compatibility re-exports for portable node-kind authoring contracts.
//!
//! The data and renderers live in `tinyflows-bus`, so a host can use them
//! without linking the engine.

pub use serde_json::Value;
pub use tinyflows_bus::node_kinds::*;

#[cfg(test)]
#[path = "catalog_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "catalog/fan_out_contract_tests.rs"]
mod fan_out_contract_tests;
