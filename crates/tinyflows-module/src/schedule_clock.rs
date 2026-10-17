//! The fixed calendar instant of a host-driven scheduler pass.

use chrono::{DateTime, Utc};
use tinyflows_schedule::runtime::Clock;

/// Keeps due selection, run records and recurring schedules on the same time.
pub(super) struct FixedClock(pub(super) DateTime<Utc>);

#[async_trait::async_trait]
impl Clock for FixedClock {
    fn now(&self) -> DateTime<Utc> {
        self.0
    }

    async fn sleep(&self, duration: std::time::Duration) {
        // Calendar time is fixed for the pass; retry backoff still yields to
        // Tokio's timer, which a test can pause and advance independently.
        tokio::time::sleep(duration).await;
    }
}
