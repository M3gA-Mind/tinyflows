# tinyflows-bus design (openhuman#8055, tinyflows#3, tinyflows#4)

Goal: the OpenHuman host reaches workflow behaviour only through the
`tinyflows-bus` contract served by the compiled `tinyflows-module`. The host
keeps config, security policy and approvals, credentials, `Outcome<T>`
controllers and event-bus wiring. This document fixes the contract shape and
the slice order. Catalog CRUD and revision history are served by the module;
run execution remains a later slice.

## Contract surfaces

One bus name (`ai.tinyhumans.tinyflows.Authoring`), one object path and
interface per surface, each with its own `CONTRACT_VERSION`:

| Surface | Status | Members |
| --- | --- | --- |
| Authoring | served, v2 | `ValidateGraph`, `ImportGraph`, `BindingFailures`, `AuthoringFailures`, `UnresolvableToolArgs`, `BuilderFailures` (migrate, compat, import, gates, preflight) |
| Schedule | served, v3 | scheduler start/stop/tick/drain, `ExecuteJob`, `ScheduleStore`, delay and delivery validation, schedule maths and validation (`NextRun`, `ValidateSchedule`, `RunsCloserThan`, `NormalizeExpression`, `ScheduleCronExpression`), `DefaultDelivery`, `CheckOriginRequirements`, `CheckPatch`, `TruncateCronOutput` |
| Runs | skeleton, v1 | `StartRun`, `ResumeRun`, `CancelRun`, `PollRun` |
| CapsHost | host callback, v1 | `LlmComplete`, `ToolInvoke`, `HttpRequest`, `CodeRun`, `ShellRun`, `MemoryRecall`, `MemoryFlavour`, `MemoryPeople`, `MemoryRemember`, `MemoryForget`, `AgentRun`, `AgentEvents`, `ResolveAgent`, `ListAgents`, `ResolveContext`, `ResolveTools`, `ResolveWorkdir`, `ApprovalDecide`, `ApprovalCancel` |
| Catalog | served, v4 | `ListWorkflows`, `GetWorkflow`, `PutWorkflow` (create/update/duplicate), `DeleteWorkflow`, `SetEnabled`, `ListRevisions`, `GetRevision`, `CreateDraft`, `GetDraft`, `UpdateDraft`, `ListDrafts`, `DeleteDraft`, `PromoteDraft`, `UpsertSuggestions`, `ListSuggestions`, `SetSuggestionStatus`, `GetRun`, `ListRuns`, `ListAllRuns`, `PruneRuns`, `NodeKinds`, `ApplyGraphOps`, `ApprovalManifest` |

Rules: contract crates stay synchronous and I/O-free; graphs and runs cross the
boundary as JSON; the host never links the engine. Adding a surface does not
change the other surfaces' versions; changing a served shape bumps that
surface's version and the registry records the pair.

### Validate / migrate / compat

`ValidateGraph` runs migration, structural validation and engine compatibility
in the module; the result carries the migrated graph even when invalid so
editors keep the author's work. A request may include the scope-bound
`AuthoringHost.ResolveWorkflow` callback. When present, the shared async
compatibility walk checks saved sub-workflow children; missing or unavailable
children retain the engine's existing runtime-diagnostic behavior.

`ImportGraph` normalizes native and n8n documents without persistence. `auto`
detects n8n exports, while `native`/`tinyflows` and `n8n` select a format
explicitly. Format names are trimmed and case-insensitive; an empty format is
the same as `auto`. The module returns importer warnings separately from
structural and engine compatibility errors; product trigger warnings remain
host-owned.

### Run / resume / cancel

`StartRun` returns a run id at once and the module drives the run on its own
executor. `ResumeRun` supplies approvals for a parked run. `CancelRun` is
cooperative (the engine's cancel token). Progress is not a transport stream:
`PollRun(run_id, cursor)` returns events with a monotonic `seq`, so the host
can republish them on its event bus and a restart resumes from its last
cursor. Run checkpoints live in the host document port (below).

### Node kinds, graph_ops, gates, preflight

The canonical host-facing `NodeKindContract` data model, host-note overlay
method, and renderers live in `tinyflows-bus::node_kinds`. The engine retains
its source-compatible API snapshot because the bus crate is intentionally
unpublished; an exhaustive module parity test keeps every contract and
renderer aligned. The `NodeKinds` member returns the full ordered schema,
including config fields, ports, examples and portable notes, without requiring
hosts to link the engine. `ApplyGraphOps` applies
tagged edits sequentially to a private graph copy and returns either the full
result or the first indexed error, with no validation or persistence.

The approval manifest follows `tinyflows-catalog::approval_manifest`: the
module lists candidates in graph order, the scope-bound
`ApprovalManifestHost.ClassifyManifestCandidates` callback classifies every
candidate slot, and the module assembles the wire entries. A missing,
malformed, or failed callback response fails the operation closed. Host policy
still owns HTTP/code/native/connector classification and curated-read skips;
dynamic and agent rows have no host classification.

## Host callbacks for caps traits

Every host-owned `tinyflows::caps` method is served by the `CapsHost`
interface, whose exact callback address the trusted host binds to the flow,
run, approval requirement, and owner scope before run execution. The module
uses that address verbatim; graph and trigger JSON cannot select or replace it.
The module never sees credentials, and `conn` stays an opaque host reference:

| Trait | Members |
| --- | --- |
| `LlmProvider` | `LlmComplete(request, conn)` |
| `ToolInvoker` | `ToolInvoke(slug, args, conn)` |
| `HttpClient` | `HttpRequest(request, conn)` |
| `CodeRunner` / `ShellRunner` | `CodeRun`, `ShellRun` |
| `MemoryProvider` | `MemoryRecall/Flavour/People/Remember/Forget` |
| Module-owned state and catalogue reads | document port (`flows_kv`, saved workflow catalog) |
| `ApprovalProvider` | `ApprovalDecide`, `ApprovalCancel` |
| Module-owned tasks | Tokio task runner with the run's capability bundle |
| `AgentRunner` | `AgentRun`, `AgentEvents` |

The module constructs `StateStore` over the run's owner-scoped document
callback, with the trusted namespace `flow:<flow_id>`, and resolves saved
workflow ids through that same scoped catalog. The run manager supplies the
flow id separately from the graph and trigger. Spawn tasks execute inside the
module: tool and HTTP tasks call the same run capability bundle, and child
workflow tasks retain the module task runner through a weak reference to avoid
a runner/capabilities ownership cycle. Task start, poll, and cancel never
bounce through the host. Saved child graphs and inline workflow-task graphs pass
the engine compatibility guard before compilation; run-time resolution fails
closed when a legacy saved graph has a topology the current executor cannot run.
If a child workflow parks for approval, its task settles as a capability failure
instead of reporting partial output as success: TaskState has no parked/resume
state, and detached tasks are process-local rather than checkpointed run state.

Only `AgentRunner` streams progress. `AgentRun` returns an opaque handle, then
`AgentEvents(handle, cursor)` reads typed events and a typed terminal outcome.
Cursors start at zero and pages are contiguous, one-based sequence numbers;
replay, gaps, pages above 256 entries, and transcripts above 10,000 entries
fail as capability errors. Empty nonterminal pages are paced by the module, and
an empty terminal page is allowed. A terminal page has exactly one outcome;
the host's settled transcript is authoritative, with bounded progress entries
used only when that transcript is empty. Agent absence (`None`) and empty tool
grants stay ordinary absence. Workdir refusal remains explicit. Transport or
malformed-response errors are capability failures, never a pending result.

Approval callbacks use the existing create-or-fetch `request_id` contract:
`Pending` parks the node for engine resume, and a later call with the same id
reads the same request. Callback failures fail closed. `CodeRun` forwards the
existing source string and JSON input unchanged, including source using the
legacy function-body `return` convention; the module does not wrap it as
stdin/stdout script input. `ShellRun` uses the closed `sh`/`bash` interpreter
and typed inline/path request.

## Schedule contract v3: typed records and pure members

A host reaches the scheduler through `tinyflows-bus` alone: it links neither
`tinyflows-schedule` nor `tinyflows-drivers`, and redeclares no type. v3 makes
that possible:

- The wire records live in `tinyflows_bus::schedule::types` (`Schedule`,
  `CronJob`, `CronRun`, `CronJobPatch`, `JobOrigin`, `DeliveryConfig`,
  `DeliveryStatus`, `JobPolicy`, `JobType`, `SessionTarget`, `ActiveHours`,
  `delivery_mode`). `tinyflows_bus::schedule` exports them together with
  `AgentJobSpec`, `ImportedJob`, the callback records
  `Attempt`/`RunRecord`/`Completion`, `StoreOperation` (the typed `op` enum),
  `TooFrequent` and the constants `MIN_AGENT_JOB_INTERVAL`,
  `MAX_CRON_OUTPUT_BYTES`, `TRUNCATED_OUTPUT_MARKER`. `tinyflows-schedule`
  re-exports every one at its old path. JSON shapes are byte-identical, pinned
  by `wire_tests` in both crates. Adds `chrono` (no I/O) to the contract.
- New members (algorithms that need the `cron` crate run in the module):
  `NextRun(NextRunRequest)`, `ValidateSchedule(ValidateScheduleRequest)`
  (`agent: true` adds the cadence floor), `RunsCloserThan(RunsCloserThanRequest)`,
  `NormalizeExpression(string)`, `ScheduleCronExpression(Schedule)`,
  `DefaultDelivery(Option<JobOrigin>)`, `CheckOriginRequirements(CheckOriginRequest)`,
  `CheckPatch(CheckPatchRequest)`, `TruncateCronOutput(string)`.
- New `ScheduleStore` operations: `set_max_active_jobs`, `import_job`,
  `import_run`.

## Storage-port decision (`tinyflows-sqlite`)

Decision: the module does not own a SQLite backend. Schedule contract v2
already removed it. The catalog and run checkpoints use the document port
(`CatalogStorage`: ensure/get/put/delete/query/delete-where over scoped collections), backed on the
host by `tinystoragedrivers` (sqlite, file, mongodb) selected by
`OPENHUMAN_STORAGE_URL`. `tinyflows-drivers` keeps the document-port
implementation of the catalog stores; `tinyflows-import` stays the one-shot
reader of legacy `.db` files. No `tinyflows-sqlite` crate is created. Scope is
the agent id, set by the host when it binds the callback object.

The served Catalog v2 receives a `CatalogStorage` callback address on each
request. That object is created by the host for the acting agent, so the module
does not accept a scope string from workflow data. The module drives
`FlowCatalogDocuments` through the callback and allowlists its catalog
collections. `PutWorkflow` distinguishes create, update and duplicate; update
uses the previously observed `updated_at` as its optional expected version and
returns a structured conflict with the current workflow. Create enforces the
generic automatic-trigger and outbound-approval safeguards and reports whether
it born-disabled the workflow or forced approval. New and duplicated workflows
start at revision 1; only a committed graph update advances the number. Existing
documents without the sequence report one plus their visible retained history;
any older, already-pruned history cannot be reconstructed. The next successful
graph update persists the following number. Imported definitions remain
unseeded while their history is copied, so a history import does not masquerade
as a graph update. Once persisted, the durable counter does not reset when old
snapshots are pruned. List rows
include the latest revision, status, trigger kind, node count, update time and
skipped corrupt-row count; revisions are served by id and history order. The
CatalogStorage port advertises only ensure, get, put, delete, query and
delete-where: the current catalog driver does not use transaction batches or
collection counts.

Catalog v3 also serves the existing draft, suggestion, and run-read behavior
through that same scope-bound document port. Draft graphs remain raw JSON and
draft patches preserve the difference between an unchanged `flow_id`, clearing
it with `null`, and assigning an id. Draft promotion is generic orchestration:
the module loads the draft and selects create or update, calls a distinct
scope-bound `DraftPromotionHost.SavePromotedDraft` callback for the normal host
save path, then best-effort deletes the draft. The callback carries typed
workflow data and opaque host outcome metadata; a structured failure retains
its code, message, and details and leaves the draft in place. The module does
not own host validation, trigger binding, approval policy or event side effects.
Suggestion and run DTOs are mirrored in the serde-only bus crate so clients do
not need implementation-library dependencies.

## Slice plan

1. Catalog CRUD, enablement, revisions and scoped document callbacks are
   served. `runs` remains vocabulary-only.
2. `NodeKinds` served; core `flows` store reads move behind
   the bus; delete the core copies and tests.
3. `ApplyGraphOps` and `ApprovalManifest` served (host classifies).
4. `CapsHost` callbacks and `StartRun`/`PollRun`/`CancelRun`; core run
   manager shrinks to the host adapter and event-bus projection.
5. `ResumeRun`, parked-run expiry, boot sweep (`run_management` pure parts
   already in `tinyflows-catalog`).
6. Retire remaining direct `tinyflows` links from core; update
   `module-boundaries.json` and the boundary inventory.

Known blocker (not fixed here): modules that depend on tinybus through a
nested path cannot be co-linked into `openhuman-modules-linked`; tracked in the
tinybus epic.
