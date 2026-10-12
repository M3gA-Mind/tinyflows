//! What an import did, record kind by record kind.

use std::collections::BTreeMap;

/// How many records of one kind an import wrote, found already present, or
/// could not decode.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Tally {
    /// Records this import wrote.
    pub written: usize,
    /// Records the target already held, left exactly as they were.
    pub existing: usize,
    /// Records in the legacy store that could not be decoded or were refused.
    pub invalid: usize,
}

/// The outcome of one `import_*` call.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ImportReport {
    kinds: BTreeMap<&'static str, Tally>,
    problems: Vec<String>,
}

impl ImportReport {
    pub(crate) fn wrote(&mut self, kind: &'static str, written: bool) {
        let tally = self.kinds.entry(kind).or_default();
        if written {
            tally.written += 1;
        } else {
            tally.existing += 1;
        }
    }

    /// Records one record that could not be imported, with why.
    pub(crate) fn invalid(&mut self, kind: &'static str, what: impl std::fmt::Display) {
        self.kinds.entry(kind).or_default().invalid += 1;
        let message = format!("{kind}: {what}");
        tracing::warn!(target: "tinyflows_import", "[import] skipped an unreadable record: {message}");
        self.problems.push(message);
    }

    /// The tally of `kind` (`"flows"`, `"runs"`, `"jobs"`, ...); all zero when
    /// the store held none.
    pub fn tally(&self, kind: &str) -> Tally {
        self.kinds.get(kind).copied().unwrap_or_default()
    }

    /// How many records of `kind` this import wrote.
    pub fn written(&self, kind: &str) -> usize {
        self.tally(kind).written
    }

    /// Every kind seen, with its tally, in name order.
    pub fn kinds(&self) -> impl Iterator<Item = (&'static str, Tally)> + '_ {
        self.kinds.iter().map(|(kind, tally)| (*kind, *tally))
    }

    /// One line per record that could not be imported.
    pub fn problems(&self) -> &[String] {
        &self.problems
    }

    /// Records written across every kind.
    pub fn total_written(&self) -> usize {
        self.kinds.values().map(|tally| tally.written).sum()
    }

    /// Whether every legacy record was either written or already present.
    pub fn is_clean(&self) -> bool {
        self.problems.is_empty()
    }
}

#[cfg(test)]
#[path = "report_tests.rs"]
mod tests;
