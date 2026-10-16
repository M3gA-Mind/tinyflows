use super::*;

#[test]
fn ids_are_ulids_in_creation_order_even_within_one_millisecond() {
    let ids: Vec<String> = (0..50).map(|_| new_id()).collect();
    assert!(ids.iter().all(|id| id.len() == 26));
    let mut sorted = ids.clone();
    sorted.sort();
    assert_eq!(ids, sorted);
}

#[test]
fn monotonic_time_never_repeats() {
    let a = monotonic_ms();
    let b = monotonic_ms();
    assert!(b > a);
}
