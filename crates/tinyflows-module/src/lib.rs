//! Loadable workflow authoring module. Hosts depend on `tinyflows-bus` and
//! invoke the compiled module; the engine stays behind that boundary.

mod host;
mod service;
mod validation;

pub mod caps;
mod catalog_authoring;
mod catalog_documents;
mod catalog_extended_ops;
mod catalog_service;
mod exports;
mod schedule_callbacks;
mod schedule_service;

mod schedule_documents;
mod schedule_store;
mod runs_service;
mod run_manager;

#[cfg(feature = "static-link")]
pub use exports::linked_module;
pub use tinyflows_bus::{BUS_NAME, OBJECT_PATH};
