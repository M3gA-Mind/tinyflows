# Workflow authoring contract

Hosts depend on this serialization-only crate and call the compiled
`tinyflows-module` through TinyBus. They do not link `tinyflows-module` or the
engine to invoke these operations.

`ValidateGraph(graph)` migrates and parses a candidate, aggregates structural
errors, then checks engine topology compatibility. Invalid graphs are ordinary
typed results; malformed JSON graph shapes carry `unparseable_graph`. A parsed
candidate is returned even when validation fails so editors can retain it.
No operation persists a graph or invokes an external capability.

`BindingFailures(graph)` returns bindings that static analysis proves cannot
resolve. Deserialization failures are method failures. Host-specific connection,
credential, tool-contract and agent gates remain the caller's responsibility.

The wire contract starts at version 1. Its identity and method constants live
in `src/lib.rs`. Build and exercise the actual dynamic artifact with
`bash scripts/verify-module-artifact.sh` from the repository root.
