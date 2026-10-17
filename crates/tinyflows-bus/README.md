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

`WiringWarnings({graph, host})` reports best-effort output-field, payload
envelope, and `split_out` path warnings. The module walks bindings with the
engine parser and asks the request-scoped host callback for each static tool's
output contract. Unknown or unavailable contracts are skipped. The host
contract carries output fields and schema, the primary array path, its payload
envelope prefix, and top-level metadata fields. The check performs no provider
lookup, authentication, mock execution, or production capability call; dynamic
`=` slugs are skipped. Whole-payload and metadata bindings stay silent. These
diagnostics are advisory and never gate draft authoring or runtime.

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
- `LookupToolContract(slug: String) -> Option<ToolContract>` returns the output
  facts available for that exact slug. Return `None` when unknown or
  unavailable. `ToolContract` has `output_fields`, optional `output_schema`,
  optional `primary_array_path`, optional `envelope_prefix`, and
  `metadata_fields`. `output_schema` describes the payload after removing the
  envelope. `primary_array_path` is relative to the full output and includes
  the envelope segment (for example, `data.issues`); an empty string identifies
  an array at the output root.

Bind the callback object to the acting identity and store for the request.
The module has no credential or account lookup implementation. Hosts can apply
native-tool argument rules and query their own agent, account, and tool catalogs
through this callback without linking the workflow engine.

The authoring wire contract is version 3. Its identity and method constants live
in `src/lib.rs`. Build and exercise the actual dynamic artifact with
`bash scripts/verify-module-artifact.sh` from the repository root.

The module also serves the version 1 `Copilot` interface. It exposes the
portable tool catalogue and invocation, builder prompt rendering, the bundled
authoring prompts and resources, trail-off fallback construction, question
detection, and backend-failure classification. `InvokeTool` carries its
`CopilotHost` callback address outside model arguments. Hosts bind that unique
callback object to the invocation's identity, store, and dispatch context; the
module forwards port requests to that object and never accepts identity,
origin, configuration, or credentials from tool arguments. A callback may keep
the host's `ToolCallOptions`, turn origin, and configuration in its captured
scope without putting them on the wire.

When the tool is `dry_run_workflow`, the exact same bus name and object path
must also serve the existing `runs::CAPS_INTERFACE` (`CapsHost`) `ToolInvoke`
member. That second interface is only the sandbox's preflight acknowledgement;
the compiled module ignores its result and creates simulated outputs from its
own mock capabilities.

The `CallPort` request is `{ operation: PortOperation, payload: Value }`. The
`PortOperation` enum and its stable wire names are in `copilot::port_operations`.
The host returns the operation's result as a JSON value or fails the call.

| Operation | Payload | Result |
| --- | --- | --- |
| `draft.get` | `{id}` | `catalog::FlowDraft` |
| `draft.create` | `{flow_id?, name, graph, origin}` | `catalog::FlowDraft` |
| `draft.update` | `{id, name?, graph?}` | `catalog::FlowDraft` |
| `flows.get` | `{id}` | `catalog::Workflow` |
| `flows.list` | `null` | `catalog::Workflow[]` |
| `flows.load_graph` | `{id}` | graph `Value` or `null` |
| `flows.create` | `{name, graph, require_approval}` | `catalog::Workflow` |
| `flows.update` | `{id, name?, graph?}` | `copilot::WrittenWorkflow` |
| `flows.set_enabled` | `{id, enabled}` | `catalog::Workflow` |
| `flows.duplicate` | `{id}` | `catalog::Workflow` |
| `flows.history` | `{id, limit}` | `catalog::WorkflowRevision[]` |
| `flows.list_runs` | `{flow_id, limit}` | `catalog::FlowRun[]` |
| `flows.get_run` | `{run_id}` | `catalog::FlowRun` |
| `flows.resume` | `{flow_id, run_id, approve, reject}` | JSON result |
| `flows.cancel_run` | `{run_id}` | JSON result |
| `agents.list` | `null` | `copilot::AgentDefinition[]` |
| `suggestions.upsert` | `catalog::FlowSuggestion[]` | inserted count |
| `gates.validate` | graph JSON | `copilot::FlowValidation` |
| `gates.run` | `WorkflowGraph` | diagnostic `string[]` |
| `gates.strict` | graph JSON | boolean; `true` passes, `false` rejects |
| `gates.engine_compatibility` | `WorkflowGraph` | diagnostic `string[]` |
| `gates.wiring_warnings` | `WorkflowGraph` | advisory `string[]` |
| `gates.approval_manifest` | `WorkflowGraph` | permission manifest `Value[]` |

The `catalog::` types are re-exported directly by `tinyflows-bus`. The
`copilot::FlowValidation`, `copilot::AgentDefinition`, and
`copilot::WrittenWorkflow` DTOs mirror the owning library's serde wire shapes;
the module tests compare those DTOs against library deserialization. Hosts can
build every callback response from bus types without linking the module or
`tinyflows-copilot`. Callback failures or invalid result
shapes fail closed for validation, strict gates, and sandbox permission
classification. Wiring warnings remain best effort.

Draft simulation is sent directly from the compiled module to the existing
`Runs.SandboxRun` operation, with the same request-scoped host object as the
preflight-only `ToolInvoke` callback. The callback cannot replace the module's
mock execution or produce simulated outputs.
