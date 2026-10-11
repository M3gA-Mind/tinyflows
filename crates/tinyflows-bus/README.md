# Workflow authoring contract

Hosts depend on this serialization-only crate and call the compiled
`tinyflows-module` through TinyBus. They do not link `tinyflows-module` or the
engine to invoke these operations.

`ValidateGraph(graph)` migrates and parses a candidate, aggregates structural
errors, then checks engine topology compatibility. Invalid graphs are ordinary
typed results; malformed JSON graph shapes carry `unparseable_graph`. A parsed
candidate is returned even when validation fails so editors can retain it.
No operation persists a graph or invokes an external capability.

`BindingFailures(graph)` reports node-output bindings missing their required
output envelope. `AuthoringFailures(graph)` runs the full portable gate set:
agent prompts, bindings, agent output schemas and code languages. It returns
engine diagnostics in their original order.

`UnresolvableToolArgs(graph, native_slug_prefixes)` mock-runs the graph and
reports outbound arguments proven to resolve to null. The host supplies its
native-tool prefixes; the module excludes those tools from this check. Mock
trigger data and opaque upstream tool output do not count as proven failures.
This check is best effort: compile errors, capability errors and timeouts
produce no diagnostics. Call `ValidateGraph` first; an empty preflight report
does not establish structural validity.

Deserialization failures in these gate operations are method failures.
Host-specific connection, credential, tool-contract and agent-reference gates
remain the caller's responsibility.

The wire contract starts at version 1. Its identity and method constants live
in `src/lib.rs`. Build and exercise the actual dynamic artifact with
`bash scripts/verify-module-artifact.sh` from the repository root.
