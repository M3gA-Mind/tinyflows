//! Run-lifecycle rules that hold for any host that persists flow runs: which
//! statuses are settled, what a cancel of a settled run says, and which scopes a
//! boot-time orphan sweep may touch. The reads and writes are the host's.

/// Statuses a run never leaves. A cancel of one is refused rather than
/// overwriting the outcome it already recorded.
pub const TERMINAL_RUN_STATUSES: &[&str] = &[
    "completed",
    "completed_with_warnings",
    "failed",
    "cancelled",
    "interrupted",
];

/// Seconds a run may stay parked in `pending_approval` before it is expired to
/// `cancelled`.
pub const PARKED_RUN_TTL_SECS: i64 = 24 * 60 * 60;

/// The recorded reason on a run reconciled by the boot orphan sweep.
pub const ORPHANED_RUN_REASON: &str =
    "Run interrupted by an app restart — no live run was executing this row after boot.";

/// Whether `status` is terminal ([`TERMINAL_RUN_STATUSES`]).
#[must_use]
pub fn is_terminal_run_status(status: &str) -> bool {
    TERMINAL_RUN_STATUSES.contains(&status)
}

/// The refusal for cancelling a run that already settled.
#[must_use]
pub fn already_terminal_error(run_id: &str, status: &str) -> String {
    format!("flow run '{run_id}' is already terminal (status: {status}) — nothing to cancel")
}

/// The refusal when a run settled between the status read and the guarded
/// cancel write: its recorded outcome was left untouched.
#[must_use]
pub fn settled_concurrently_error(run_id: &str) -> String {
    format!(
        "flow run '{run_id}' settled before it could be cancelled — its recorded outcome was \
         left untouched"
    )
}

/// The recorded error on a parked run expired after `ttl_secs`.
#[must_use]
pub fn parked_expiry_message(ttl_secs: i64) -> String {
    format!("parked run expired after {ttl_secs}s awaiting approval")
}

/// Which scopes the boot orphan sweep reconciles.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootSweepPlan {
    /// The local scope, then every agent scope.
    EveryScope,
    /// No scope.
    Nothing,
}

/// The boot sweep's scopes. On a backend other processes share, a `running` row
/// below the boot floor can belong to a run another replica still drives, and
/// sweeping it would drop that run's checkpoint, so nothing is swept there.
#[must_use]
pub fn boot_sweep_plan(shared_backend: bool) -> BootSweepPlan {
    if shared_backend {
        BootSweepPlan::Nothing
    } else {
        BootSweepPlan::EveryScope
    }
}

#[cfg(test)]
#[path = "run_management_tests.rs"]
mod tests;
