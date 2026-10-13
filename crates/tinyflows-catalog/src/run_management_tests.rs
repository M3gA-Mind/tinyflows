use super::*;

#[test]
fn settled_statuses_are_terminal_and_live_ones_are_not() {
    for s in ["completed", "completed_with_warnings", "failed", "cancelled", "interrupted"] {
        assert!(is_terminal_run_status(s), "{s}");
    }
    for s in ["running", "pending_approval", ""] {
        assert!(!is_terminal_run_status(s), "{s}");
    }
}

#[test]
fn messages_name_the_run_and_status() {
    assert!(already_terminal_error("r1", "failed").contains("'r1'"));
    assert!(already_terminal_error("r1", "failed").contains("status: failed"));
    assert!(settled_concurrently_error("r2").contains("'r2'"));
    assert_eq!(parked_expiry_message(60), "parked run expired after 60s awaiting approval");
}

#[test]
fn boot_sweep_skips_a_shared_backend() {
    assert_eq!(boot_sweep_plan(false), BootSweepPlan::EveryScope);
    assert_eq!(boot_sweep_plan(true), BootSweepPlan::Nothing);
}
