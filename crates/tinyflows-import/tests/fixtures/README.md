# Legacy store fixtures

One store of every retired kind, written by the retired writers themselves
(tinyflows at `953d541`, v0.8.8: `tinyflows-sqlite`, `FileCheckpointer`,
`FileStateStore`, `FileWorkflowStore`), so the importer is tested against bytes
the old code produced and not against a format reconstructed from reading it.

| Path | Written by | Holds |
| --- | --- | --- |
| `catalog/flows.db`, `catalog/drafts/*.json` | `tinyflows_sqlite::flows`, the draft store | two flows (one disabled and approval-gated), two revisions of `flow-alpha`, four runs (completed with a step, parked with an approval and a graph hash, live, failed), three state entries, two suggestions (one dismissed), two drafts |
| `cron/jobs.db` | `tinyflows_sqlite::schedule` | a named shell job, an agent job with a channel origin, a flow-schedule job, four runs with delivery statuses |
| `checkpoints/checkpoints.db`, `checkpoints/file/` | `SqliteCheckpointer`, `FileCheckpointer` | the same data in both: a thread with a subgraph namespace, a thread whose id needs escaping, three pending writes; the `file/` copy ends in a torn line |
| `file-state/` | `FileStateStore` | two keys under `workflow:flow-alpha` |
| `workflow-store/` | `FileWorkflowStore::with_state` | two workflows, three revisions (one left by a delete), three runs, a two-note journal with a supersession, a proposal |
| `expected.json` | the same stores' own readers | what each store returned when read back, which the tests compare the imported data to |

Nothing here is ever written to: the tests that import from this tree hash every
file under it before and after. The `workflow-store/state/definitions/<hash>`
directory is named by a hash of the absolute path the fixture was generated at,
so the tests find it rather than derive it; the derivation itself is covered by
tests over temporary directories.
