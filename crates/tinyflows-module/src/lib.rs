//! Loadable workflow authoring module. Hosts depend on `tinyflows-bus` and
//! invoke the compiled module; the engine stays behind that boundary.

mod host;
mod service;
mod validation;
