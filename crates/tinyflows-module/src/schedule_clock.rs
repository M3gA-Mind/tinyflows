//! The calendar instant of a host-driven scheduler.

use chrono::{DateTime, Utc};
use std::sync::Mutex;
use tinyflows_schedule::runtime::Clock;

/// The calendar time of one manual scheduler.
///
/// Without a host-supplied instant it reads system time. `config.now` fixes it
/// at start, and every `TickSchedulerAt` moves it to the instant the host
/// passes, so due selection, run records and rescheduling in that pass all see
/// the host's clock: a host can step a schedule forward without waiting for it.
pub(super) struct HostClock(Mutex<Option<DateTime<Utc>>>);

impl HostClock {
    /// A clock on system time until the host supplies an instant.
    pub(super) fn system() -> Self {
        Self(Mutex::new(None))
    }

    /// A clock fixed at `now` until the host supplies another instant.
    pub(super) fn fixed(now: DateTime<Utc>) -> Self {
        Self(Mutex::new(Some(now)))
    }

    /// Moves calendar time to `now` for the next pass.
    pub(super) fn set(&self, now: DateTime<Utc>) {
        *self.0.lock().expect("scheduler clock poisoned") = Some(now);
    }
}

#[async_trait::async_trait]
impl Clock for HostClock {
    fn now(&self) -> DateTime<Utc> {
        self.0
            .lock()
            .expect("scheduler clock poisoned")
            .unwrap_or_else(Utc::now)
    }

    async fn sleep(&self, duration: std::time::Duration) {
        // Calendar time belongs to the host; retry backoff still yields to
        // Tokio's timer, which a test can pause and advance independently.
        tokio::time::sleep(duration).await;
    }
}
