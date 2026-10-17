use super::*;

#[test]
fn keys_are_unambiguous_and_long_ones_hashed() {
    assert_ne!(key(&["a/b", "c"]).unwrap(), key(&["a", "b/c"]).unwrap());
    assert_ne!(key(&["", "x"]).unwrap(), key(&["x", ""]).unwrap());
    let hashed = key(&[&"t".repeat(600)]).unwrap();
    assert!(hashed.starts_with('~') && hashed.len() == 53, "{hashed}");
    assert_ne!(hashed, key(&[&"u".repeat(600)]).unwrap());
}

#[test]
fn sequence_ids_are_their_own_keys() {
    assert_eq!(key(&["ldg_00000007"]).unwrap(), "ldg_00000007");
    assert_eq!(key(&["les_00000001"]).unwrap(), "les_00000001");
}

#[test]
fn only_a_conflict_is_a_race() {
    assert!(is_race(&StorageError::conflict("x")));
    assert!(!is_race(&StorageError::unavailable("x")));
}

#[test]
fn instants_round_trip_through_milliseconds() {
    use super::convert::{instant_ms, rfc3339};
    assert_eq!(
        rfc3339(instant_ms("2026-01-01T00:00:05Z")),
        "2026-01-01T00:00:05Z"
    );
    assert_eq!(
        rfc3339(instant_ms("2026-01-01T02:00:05.250+02:00")),
        "2026-01-01T00:00:05.250Z"
    );
    assert_eq!(instant_ms("not a time"), 0);
}
