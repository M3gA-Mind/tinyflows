//! The cap on a journal: what stays when automation writes more than fits.

use super::*;
use crate::store::types::{NoteKind, NoteSource};

fn note(recorded_at: u64, text: &str) -> WorkflowNote {
    WorkflowNote {
        id: mint_id(recorded_at),
        workflow_id: "sweep".to_string(),
        kind: NoteKind::Observation,
        text: text.to_string(),
        recorded_at,
        source: NoteSource::System,
        run_ids: Vec::new(),
        superseded_by: None,
        pinned: false,
    }
}

/// What appending `incoming` to a journal does: push, then prune.
fn append(notes: &mut Vec<WorkflowNote>, incoming: WorkflowNote) {
    notes.push(incoming);
    prune(notes);
}

fn supersede(notes: &mut [WorkflowNote], id: &str, by: &str) {
    for note in notes.iter_mut().filter(|n| n.id == id) {
        note.superseded_by = Some(by.to_string());
    }
}

#[test]
fn a_journal_under_the_cap_is_left_alone() {
    let mut notes: Vec<_> = (0..MAX_NOTES as u64).map(|at| note(at, "n")).collect();
    prune(&mut notes);
    assert_eq!(notes.len(), MAX_NOTES);
}

#[test]
fn the_journal_is_capped_and_drops_the_oldest_first() {
    let mut notes = Vec::new();
    for at in 0..(MAX_NOTES as u64 + 5) {
        append(&mut notes, note(at, &format!("note {at}")));
    }
    notes.sort_by(|a, b| b.id.cmp(&a.id));
    assert_eq!(notes.len(), MAX_NOTES);
    assert_eq!(
        notes.last().expect("a note").text,
        "note 5",
        "the five oldest went, not the five newest"
    );
}

#[test]
fn pinned_notes_survive_the_cap() {
    let mut notes = Vec::new();
    let mut pinned = note(0, "what the operator said");
    pinned.pinned = true;
    append(&mut notes, pinned.clone());
    for at in 1..(MAX_NOTES as u64 + 20) {
        append(&mut notes, note(at, &format!("note {at}")));
    }
    assert_eq!(notes.len(), MAX_NOTES);
    assert!(
        notes.iter().any(|n| n.id == pinned.id),
        "automation writing a hundred observations must not evict a person's note"
    );
}

#[test]
fn an_old_supersession_chain_is_evicted_together_at_the_cap() {
    let mut notes = Vec::new();
    let first = note(0, "obsolete");
    let replacement = note(1, "current");
    append(&mut notes, first.clone());
    append(&mut notes, replacement.clone());
    supersede(&mut notes, &first.id, &replacement.id);
    for at in 2..(MAX_NOTES as u64 + 20) {
        append(&mut notes, note(at, &format!("note {at}")));
    }
    assert_eq!(notes.len(), MAX_NOTES);
    assert!(!notes.iter().any(|n| n.id == first.id));
    assert!(!notes.iter().any(|n| n.id == replacement.id));
    assert!(notes.iter().all(|n| {
        n.superseded_by
            .as_ref()
            .is_none_or(|id| notes.iter().any(|replacement| &replacement.id == id))
    }));
}

#[test]
fn a_recent_replacement_survives_with_its_superseded_predecessor() {
    let mut notes = Vec::new();
    for at in 0..MAX_NOTES as u64 {
        append(&mut notes, note(at, &format!("note {at}")));
    }
    let first = note(MAX_NOTES as u64, "obsolete");
    let replacement = note(MAX_NOTES as u64 + 1, "current");
    append(&mut notes, first.clone());
    append(&mut notes, replacement.clone());
    supersede(&mut notes, &first.id, &replacement.id);
    prune(&mut notes);
    assert_eq!(notes.len(), MAX_NOTES);
    assert!(notes.iter().any(|n| n.id == first.id));
    assert!(notes.iter().any(|n| n.id == replacement.id));
}

#[test]
fn ids_minted_in_the_same_millisecond_still_sort_in_order() {
    // A pass writes several notes at once; without the counter their order
    // would fall through to a random token and the listing would be arbitrary.
    let ids: Vec<String> = (0..8).map(|_| mint_id(1_700_000_000_000)).collect();
    let mut sorted = ids.clone();
    sorted.sort();
    assert_eq!(ids, sorted);
}

#[test]
fn a_note_round_trips_every_field_through_json() {
    let written = WorkflowNote {
        id: mint_id(7),
        workflow_id: "sweep".into(),
        kind: NoteKind::Constraint,
        text: "the deploy step must never run before tests".into(),
        recorded_at: 7,
        source: NoteSource::Agent {
            model: Some("claude-opus-5".into()),
        },
        run_ids: vec!["run-1".into(), "run-2".into()],
        superseded_by: None,
        pinned: true,
    };
    let json = serde_json::to_string(&written).expect("serialize");
    let read_back: WorkflowNote = serde_json::from_str(&json).expect("parse");
    assert_eq!(read_back, written);
}
