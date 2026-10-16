# tinyflows-bus design (openhuman#7562, epic #7544, tinyflows#132)

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
| Authoring | served, v1 | `ValidateGraph`, `BindingFailures`, `AuthoringFailures`, `UnresolvableToolArgs`, `BuilderFailures` (migrate, compat, graph ops gates, preflight) |
| Schedule | served, v3 | scheduler start/stop/tick/drain, `ExecuteJob`, `ScheduleStore`, delay and delivery validation, schedule maths and validation (`NextRun`, `ValidateSchedule`, `RunsCloserThan`, `NormalizeExpression`, `ScheduleCronExpression`), `DefaultDelivery`, `CheckOriginRequirements`, `CheckPatch`, `TruncateCronOutput` |
| Runs | skeleton, v1 | `StartRun`, `ResumeRun`, `CancelRun`, `PollRun` |
| Catalog | served, v2 | `ListWorkflows`, `GetWorkflow`, `PutWorkflow` (create/update/duplicate), `DeleteWorkflow`, `SetEnabled`, `ListRevisions`, `GetRevision` |

Rules: contract crates stay synchronous and I/O-free; graphs and runs cross the
boundary as JSON; the host never links the engine. Adding a surface does not
change the other surfaces' versions; changing a served shape bumps that
surface's version and the registry records the pair.

### Validate / migrate / compat

Already served as `ValidateGraph`. Migration, structural validation and engine
compatibility run in the module; the result carries the migrated graph even
when invalid so editors keep the author's work.

### Run / resume / cancel

`StartRun` returns a run id at once and the module drives the run on its own
executor. `ResumeRun` supplies approvals for a parked run. `CancelRun` is
cooperative (the engine's cancel token). Progress is not a transport stream:
`PollRun(run_id, cursor)` returns events with a monotonic `seq`, so the host
can republish them on its event bus and a restart resumes from its last
cursor. Run checkpoints live in the host document port (below).

### Node kinds, graph_ops, gates, preflight

`NodeKinds` returns each kind's config schema and ports so the host stops
mirroring the enum. `ApplyGraphOps` wraps `graph_ops` (pure, deterministic).
Gates and mock-run preflight are `BuilderFailures` (served). The approval
manifest splits as already landed in `tinyflows-catalog::approval_manifest`:
the module lists candidates, the host classifies each with its security
policy, the module assembles the wire entries.

## Host callbacks for caps traits

Every `tinyflows::caps` trait method is one member on the host callback
interface `CapsHost`, bound per request to the acting identity (the module
never sees credentials; `conn` stays an opaque host reference):

| Trait | Members |
| --- | --- |
| `LlmProvider` | `LlmComplete(request, conn)` |
| `ToolInvoker` | `ToolInvoke(slug, args, conn)` |
| `HttpClient` | `HttpRequest(request, conn)` |
| `CodeRunner` / `ShellRunner` | `CodeRun`, `ShellRun` |
| `MemoryProvider` | `MemoryRecall/Flavour/People/Remember/Forget` |
| `WorkflowResolver` | `ResolveWorkflow` (shared with authoring host) |
| `StateStore` | `StateLoad`, `StateStore` |
| `ApprovalProvider` | `ApprovalDecide`, `ApprovalCancel` |
| `TaskRunner` | `TaskStart`, `TaskPoll`, `TaskCancel` (already poll based) |
| `AgentRunner` | `AgentRun`, `AgentEvents` |

Streaming: only `AgentRunner` streams (progress, partial output). A callback
cannot stream over a synchronous bus, so `AgentRun` starts the turn and
returns a handle; the module reads `AgentEvents(handle, cursor)` the same way
the host reads `PollRun`. Calls are blocking from the module's executor
thread. A callback failure surfaces as the node's error; approval callbacks
fail closed (an error is a rejection, never an approval).

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
graph update persists the following number. Once persisted, the durable counter
does not reset when old snapshots are pruned. Imported history advances the
sequence once per newly imported snapshot. List rows
include the latest revision, status, trigger kind, node count, update time and
skipped corrupt-row count; revisions are served by id and history order. The
CatalogStorage port advertises only ensure, get, put, delete, query and
delete-where: the current catalog driver does not use transaction batches or
collection counts.

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
