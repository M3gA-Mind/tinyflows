//! One-time, read-only import of the stores tinyflows hosts kept before storage
//! moved onto the `tinystoragedrivers` ports.
//!
//! Each `import_*` function reads one kind of legacy store and writes what it
//! held into the matching port-backed store from `tinyflows-drivers`:
//!
//! | Legacy | Function | Target |
//! | --- | --- | --- |
//! | `flows.db` (definitions, revisions, runs, state, suggestions) and `drafts/*.json` | [`import_flow_catalog`] | [`FlowCatalogDocuments`] |
//! | `jobs.db` (cron jobs and runs) | [`import_cron`] | [`CronDocuments`] |
//! | `checkpoints.db` | [`import_sqlite_checkpoints`] | [`DriverCheckpointer`] |
//! | `FileCheckpointer` directory (`*.jsonl`) | [`import_file_checkpoints`] | [`DriverCheckpointer`] |
//! | `FileStateStore` directory | [`import_file_state`] | [`FlowCatalogDocuments`] |
//! | `FileWorkflowStore` (definitions, runs, journal, proposals, revisions) | [`import_workflow_store`] | [`DriverWorkflowStore`] |
//!
//! # Guarantees
//!
//! - **Read-only.** Legacy files are never modified. A SQLite file is copied
//!   (with its `-wal` and `-shm`) to a scratch directory and read there, so the
//!   driver's own opening of it (WAL mode, a busy timeout) never touches the
//!   original. JSON and JSONL files are only read.
//! - **Id-preserving.** Every record lands under the id it had. The exception
//!   is a flow-schedule cron job, which the document store files under the
//!   deterministic id it derives from the flow (see
//!   [`CronDocuments::import_job`]); its runs follow it.
//! - **Only if absent.** Nothing already in the target is replaced, so the
//!   import can be repeated after a failure, or run against a scope that has
//!   been written to since, without clobbering anything. The report counts what
//!   was written and what was already there.
//! - **Forgiving about records, strict about files.** A record that cannot be
//!   decoded (a graph from a newer build, a corrupt row) is counted as invalid,
//!   logged and skipped, so one bad row costs only itself. A file that cannot
//!   be opened at all is an error, and the host decides whether to retry. A
//!   store that is not there is simply an empty report.
//!
//! # Features
//!
//! `schedule` (`import_cron`), `catalog` (`import_flow_catalog`,
//! `import_file_state`), `engine` (checkpoints) and `store`
//! (`import_workflow_store`) are all on by default. A host without the flows
//! engine builds with `default-features = false, features = ["schedule"]` and
//! still imports its legacy cron jobs. A host that only needs legacy cron
//! records can use `schedule-read`; it exposes bus DTOs without linking the
//! scheduler or `tinyflows-drivers` implementations.
//!
//! The host decides when to run this and records that it has (a marker in its
//! own storage); this crate keeps no state of its own.
//!
//! [`FlowCatalogDocuments`]: tinyflows_drivers::catalog::FlowCatalogDocuments
//! [`CronDocuments`]: tinyflows_drivers::schedule::CronDocuments
//! [`DriverCheckpointer`]: tinyflows_drivers::DriverCheckpointer
//! [`DriverWorkflowStore`]: tinyflows_drivers::store::DriverWorkflowStore

#[cfg(feature = "catalog")]
mod catalog;
#[cfg(feature = "engine")]
mod checkpoints;
#[cfg(feature = "schedule")]
mod cron;
#[cfg(feature = "flow-write")]
mod legacy_batch;
#[cfg(feature = "flow-read")]
mod flow_reader;
#[cfg(feature = "schedule-read")]
mod legacy_cron;
#[cfg(any(feature = "flow-read", feature = "schedule-read"))]
mod legacy_db;
mod report;
#[cfg(feature = "catalog")]
mod state;
#[cfg(feature = "store")]
mod workflow_store;

#[cfg(all(
    test,
    any(
        feature = "engine",
        feature = "catalog",
        feature = "schedule",
        feature = "store"
    )
))]
mod test_support;

#[cfg(feature = "catalog")]
pub use catalog::import_flow_catalog;
#[cfg(feature = "engine")]
pub use checkpoints::{import_file_checkpoints, import_sqlite_checkpoints};
#[cfg(feature = "schedule")]
pub use cron::import_cron;
#[cfg(feature = "flow-write")]
pub use legacy_batch::apply_legacy_batch;
#[cfg(feature = "flow-read")]
pub use flow_reader::read_legacy_flow_catalog;
#[cfg(feature = "schedule-read")]
pub use legacy_cron::{LegacyCron, read_legacy_cron};
pub use report::{ImportReport, Tally};
#[cfg(feature = "catalog")]
pub use state::import_file_state;
#[cfg(feature = "store")]
pub use workflow_store::{FileStoreLayout, import_workflow_store};
