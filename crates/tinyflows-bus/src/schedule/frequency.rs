//! Cadence-floor vocabulary shared by schedule validation and its hosts.
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;

/// The shortest interval a host should allow between runs of an agent job.
/// Enforced by the module's `ValidateSchedule` (agent mode) member.
pub const MIN_AGENT_JOB_INTERVAL: ChronoDuration = ChronoDuration::minutes(5);

/// Why a schedule runs more often than a threshold allows. Carries the
/// evidence, not only the verdict, so a log line or an error can quote it.
///
/// Wire shape: `{"fixed_interval": <ms>}` or
/// `{"consecutive_runs": {"first": RFC3339, "second": RFC3339}}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TooFrequent {
    /// A fixed `every_ms` interval shorter than the threshold.
    FixedInterval(#[serde(with = "millis")] ChronoDuration),
    /// Two consecutive cron occurrences closer together than the threshold.
    ConsecutiveRuns {
        /// First of the pair.
        first: DateTime<Utc>,
        /// Second of the pair.
        second: DateTime<Utc>,
    },
}

mod millis {
    use chrono::Duration;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(gap: &Duration, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_i64(gap.num_milliseconds())
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
        Duration::try_milliseconds(i64::deserialize(d)?)
            .ok_or_else(|| serde::de::Error::custom("gap out of range"))
    }
}

impl TooFrequent {
    /// The offending gap.
    pub fn gap(&self) -> ChronoDuration {
        match self {
            Self::FixedInterval(gap) => *gap,
            Self::ConsecutiveRuns { first, second } => *second - *first,
        }
    }
}

impl fmt::Display for TooFrequent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FixedInterval(gap) => write!(f, "fires every {}", describe_gap(*gap)),
            Self::ConsecutiveRuns { first, second } => write!(
                f,
                "fires at {} and again at {}, {} apart",
                first.format("%Y-%m-%d %H:%M:%S UTC"),
                second.format("%Y-%m-%d %H:%M:%S UTC"),
                describe_gap(*second - *first)
            ),
        }
    }
}

/// `4 minutes`, `30 seconds`, `1 minute 30 seconds` — whole seconds only.
pub fn describe_gap(gap: ChronoDuration) -> String {
    fn count(n: i64, unit: &str) -> String {
        if n == 1 {
            format!("{n} {unit}")
        } else {
            format!("{n} {unit}s")
        }
    }
    let seconds = gap.num_seconds();
    match (seconds / 60, seconds % 60) {
        (0, s) => count(s, "second"),
        (m, 0) => count(m, "minute"),
        (m, s) => format!("{} {}", count(m, "minute"), count(s, "second")),
    }
}
