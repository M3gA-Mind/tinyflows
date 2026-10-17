//! Process-wide scheduler-loop slots, released on stop or drop.
use std::collections::HashSet;
use std::sync::{Mutex, OnceLock};

fn loops() -> &'static Mutex<HashSet<String>> {
    static LOOPS: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    LOOPS.get_or_init(Mutex::default)
}

/// One scheduler per scoped store. Job admission itself uses durable leases.
#[must_use]
pub struct SchedulerSlot(String);
impl SchedulerSlot {
    /// Acquire the scoped poll-loop slot until this guard drops.
    pub fn acquire(scope: &str) -> Option<Self> {
        loops()
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(scope.into())
            .then(|| Self(scope.into()))
    }
    /// Whether a poll-loop slot is held in this process.
    pub fn is_running(scope: &str) -> bool {
        loops()
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains(scope)
    }
}
impl Drop for SchedulerSlot {
    fn drop(&mut self) {
        loops()
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.0);
    }
}
