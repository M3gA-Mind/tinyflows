use super::*;

#[test]
fn an_empty_report_is_clean_and_all_zero() {
    let report = ImportReport::default();
    assert!(report.is_clean());
    assert_eq!(report.tally("flows"), Tally::default());
    assert_eq!(report.total_written(), 0);
    assert_eq!(report.kinds().count(), 0);
}

#[test]
fn written_and_existing_are_counted_per_kind() {
    let mut report = ImportReport::default();
    report.wrote("flows", true);
    report.wrote("flows", true);
    report.wrote("flows", false);
    report.wrote("runs", true);
    assert_eq!(
        report.tally("flows"),
        Tally {
            written: 2,
            existing: 1,
            invalid: 0
        }
    );
    assert_eq!(report.written("runs"), 1);
    assert_eq!(report.total_written(), 3);
    let kinds: Vec<_> = report.kinds().map(|(kind, _)| kind).collect();
    assert_eq!(kinds, ["flows", "runs"]);
}

#[test]
fn an_invalid_record_is_counted_and_named() {
    let mut report = ImportReport::default();
    report.invalid("flows", "flow 'f' graph does not decode");
    assert!(!report.is_clean());
    assert_eq!(report.tally("flows").invalid, 1);
    assert_eq!(report.problems(), ["flows: flow 'f' graph does not decode"]);
}
