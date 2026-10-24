//! Ids and caps a workflow store shares, whatever it keeps its records in.
//!
//! Revision and proposal ids lead with a zero-padded stamp so a lexical sort is
//! a chronological one, then a monotonic counter (several are minted inside one
//! millisecond), then a random token (two processes can pick the same counter).
//! Note ids are minted by [`crate::store::mint_note_id`] the same way.

use crate::store::types::ProposalId;

/// How many superseded copies of one workflow are kept.
///
/// History is for taking back a mistake that was just made, not an archive:
/// past this, an operator is not undoing an edit they just watched happen.
pub const MAX_REVISIONS: usize = 20;

/// Tie-breaker for revisions taken inside the same millisecond.
static REVISION_SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Tie-breaker for proposals minted inside the same millisecond.
static PROPOSAL_SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Mint the id of a revision superseded at `superseded_at` (epoch ms).
pub fn mint_revision_id(superseded_at: u64) -> String {
    format!(
        "{superseded_at:013}-{:012}-{}",
        REVISION_SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        crate::ids::token()
    )
}

/// Mint a proposal id that sorts chronologically.
pub fn mint_proposal_id(created_at: u64) -> ProposalId {
    format!(
        "{created_at:013}-{:012}-{}",
        PROPOSAL_SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        crate::ids::token()
    )
}

#[cfg(test)]
#[path = "ids_tests.rs"]
mod tests;
