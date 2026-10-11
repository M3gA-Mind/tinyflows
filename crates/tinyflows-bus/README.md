# Workflow authoring contract

Hosts depend on this serialization-only crate and call the compiled
`tinyflows-module` through TinyBus. They do not link `tinyflows-module` or the
engine to invoke these operations.

`ValidateGraph(graph)` migrates and parses a candidate, aggregates structural
errors, then checks engine topology compatibility. Invalid graphs are ordinary
typed results; malformed JSON graph shapes carry `unparseable_graph`. A parsed
candidate is returned even when validation fails so editors can retain it.
No operation persists a graph or executes a production workflow capability.
`BuilderFailures` may call the host lookup and authoring-policy ports described below.

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
`BuilderFailures(graph, host, native_slug_prefixes)` owns the ordered gate
composition. Call `ValidateGraph` first. It returns diagnostics from the first
failing stage, in this order: engine compatibility, saved-workflow
compatibility, portable authoring gates, host tool policy, host agent
references, host connections, host tool contracts, and mock argument preflight.
The module passes host diagnostics through verbatim. Provider connectivity
remains a runtime check or a separate advisory; it is not a hard authoring gate.

The `host` object has `bus_name` and `object_path` fields. The destination serves
`ai.tinyhumans.tinyflows.AuthoringHost` with these methods:

- `ResolveWorkflow(id: String) -> Option<Value>` reads a saved graph from the
  request's scoped store. Lookup errors, missing graphs, and unparseable saved
  graphs are skipped, matching the engine's existing saved-workflow gate. Their
  runtime missing-reference diagnostics remain authoritative.
- `CheckGate(stage: String, graph: Value) -> Vec<String>` implements product
  checks for `tool_policy`, `agent_references`, `connections`, and
  `tool_contracts`. An empty list passes the stage. A method failure fails the
  whole operation; it cannot silently pass the gate.

Bind the callback object to the acting identity and store for the request.
The module has no credential or account lookup implementation. Hosts can apply
native-tool argument rules and query their own agent, account, and tool catalogs
through this callback without linking the workflow engine.

The wire contract starts at version 1. Its identity and method constants live
in `src/lib.rs`. Build and exercise the actual dynamic artifact with
`bash scripts/verify-module-artifact.sh` from the repository root.
