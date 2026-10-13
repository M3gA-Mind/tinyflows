use super::*;

#[test]
fn revision_ids_minted_in_the_same_millisecond_sort_in_order() {
    let ids: Vec<String> = (0..8)
        .map(|_| mint_revision_id(1_700_000_000_000))
        .collect();
    let mut sorted = ids.clone();
    sorted.sort();
    assert_eq!(ids, sorted);
}

#[test]
fn revision_ids_sort_by_when_they_were_superseded() {
    assert!(mint_revision_id(1) < mint_revision_id(2));
}

#[test]
fn proposal_ids_minted_in_the_same_millisecond_sort_in_order() {
    let ids: Vec<String> = (0..8)
        .map(|_| mint_proposal_id(1_700_000_000_000))
        .collect();
    let mut sorted = ids.clone();
    sorted.sort();
    assert_eq!(ids, sorted);
}

#[test]
fn proposal_ids_are_unique_even_when_the_stamp_repeats() {
    assert_ne!(mint_proposal_id(5), mint_proposal_id(5));
}
