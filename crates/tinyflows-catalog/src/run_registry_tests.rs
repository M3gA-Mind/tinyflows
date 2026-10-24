// NOTE: `duplicate_registration_*` below pin the identity-based removal —
// see `register`'s doc for why `flows_resume` made this reachable.

use super::*;

#[test]
fn cancel_signals_a_registered_run_then_deregisters_on_drop() {
    let run_id = "flow:reg-test:run-1";
    let (token, guard) = register(run_id);
    assert!(!token.is_cancelled());

    // A live run is signalled and its token trips.
    assert!(cancel(run_id), "a registered run must be signalled");
    assert!(token.is_cancelled(), "the run's token must be cancelled");

    // Dropping the guard removes it; a second cancel finds nothing.
    drop(guard);
    assert!(
        !cancel(run_id),
        "after the guard drops the run must no longer be in flight"
    );
}

#[test]
fn cancel_of_unknown_run_is_false() {
    assert!(!cancel("flow:never-registered:run-x"));
}

/// The loser of a duplicate registration must NOT deregister the winner's
/// live token. Before removal compared registration identity, the loser's
/// guard dropping would clear the map entry the winner still relies on, and
/// a later `cancel` would report "not in flight" for a genuinely running
/// run — letting the caller drop its checkpoint mid-execution.
#[test]
fn a_displaced_guard_does_not_deregister_the_live_registration() {
    let run_id = "dup-resume-run";
    let (_winner_token, winner_guard) = register(run_id);
    // A second concurrent resume of the SAME run id registers on top.
    let (_later_token, later_guard) = register(run_id);

    // The first (now displaced) guard drops, e.g. because it lost the
    // guarded `mark_run_resuming` race and returned early.
    drop(winner_guard);

    assert!(
        is_in_flight(run_id),
        "the newer, still-live registration must survive a displaced guard's drop"
    );
    assert!(cancel(run_id), "the live run must still be cancellable");

    drop(later_guard);
    assert!(
        !is_in_flight(run_id),
        "the owning guard must still deregister its own entry"
    );
}

/// The ordinary single-registration path is unchanged.
#[test]
fn the_owning_guard_still_deregisters_its_own_entry() {
    let run_id = "solo-run";
    let (_token, guard) = register(run_id);
    assert!(is_in_flight(run_id));
    drop(guard);
    assert!(!is_in_flight(run_id));
}

/// A stable run id can be submitted to resume more than once. Only the first
/// claimant may own its cancellation registration: a later claimant must be
/// rejected rather than replacing the token of the resume that can win the
/// durable row compare-and-swap.
#[test]
fn duplicate_resume_reservation_keeps_the_winning_run_cancellable() {
    let run_id = "duplicate-resume-reservation-run";
    let (winner_token, winner_guard) =
        try_register(run_id).expect("first resume must reserve the run id");

    // Models a second resume reaching registration before the first row CAS
    // returns. It must lose at reservation time without changing the owner.
    assert!(
        try_register(run_id).is_none(),
        "duplicate resume reservations must not replace the winning token"
    );
    assert!(is_in_flight(run_id));
    assert!(cancel(run_id), "the winning resume must remain cancellable");
    assert!(winner_token.is_cancelled());

    drop(winner_guard);
    assert!(!is_in_flight(run_id));
}
