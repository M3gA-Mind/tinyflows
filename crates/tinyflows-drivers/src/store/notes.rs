//! `wf_notes`: one workflow's journal, kept as a single document updated by
//! compare-and-swap.
//!
//! Notes are only ever read as a whole set (a brief wants all of them or none)
//! and are capped, so one document per workflow is the right grain.

use serde_json::{Value, json};
use tinyflows::store::prune_notes;
use tinyflows::store::types::{WorkflowError, WorkflowNote};
use tinystoragedrivers_core::{Precondition, Versioned};

use super::definitions::is_race;
use super::{CAS_ATTEMPTS, Inner, NOTES, QUARANTINE, doc_id, malformed, storage};

impl Inner {
    /// The journal document of `workflow_id` and the notes it holds. A journal
    /// that cannot be parsed reads as empty: one bad document must not make a
    /// workflow unreadable everywhere its notes are shown, which is a worse
    /// failure than forgetting what it learned. The unreadable text is kept in
    /// `wf_quarantine`, not overwritten, so whatever an operator could still
    /// recover is not destroyed by the next append.
    async fn journal(
        &self,
        workflow_id: &str,
    ) -> Result<(Option<Versioned<Value>>, Vec<WorkflowNote>), WorkflowError> {
        self.ensure().await?;
        let Some(stored) = self
            .docs
            .get(NOTES, &doc_id(workflow_id)?)
            .await
            .map_err(storage)?
        else {
            return Ok((None, Vec::new()));
        };
        let parsed = stored
            .doc
            .get("notes_json")
            .and_then(Value::as_str)
            .map(serde_json::from_str::<Vec<WorkflowNote>>);
        match parsed {
            Some(Ok(notes)) => Ok((Some(stored), notes)),
            Some(Err(_)) | None => {
                let token = tinyflows::ids::token();
                tracing::warn!(
                    target: "workflows",
                    workflow_id,
                    kept = %token,
                    "[workflows] workflow journal is unreadable; set aside and starting a new one"
                );
                let kept = json!({ "workflow_id": workflow_id, "journal": stored.doc });
                self.docs
                    .put(QUARANTINE, &token, kept, Precondition::Absent)
                    .await
                    .map_err(storage)?;
                Ok((Some(stored), Vec::new()))
            }
        }
    }

    /// Every note for `workflow_id`, newest first, superseded ones included.
    pub(super) async fn list_notes(
        &self,
        workflow_id: &str,
    ) -> Result<Vec<WorkflowNote>, WorkflowError> {
        let (_, mut notes) = self.journal(workflow_id).await?;
        notes.sort_by(|a, b| b.id.cmp(&a.id));
        Ok(notes)
    }

    /// Appends `note`, then prunes to the cap.
    pub(super) async fn append_note(&self, note: &WorkflowNote) -> Result<(), WorkflowError> {
        self.update_journal(&note.workflow_id, |notes| {
            notes.push(note.clone());
            prune_notes(notes);
            true
        })
        .await
        .map(|_| ())
    }

    /// Marks `note_id` as replaced by `by`, returning whether a current note
    /// changed. A note that is not there is not a failure: supersession is a
    /// tidying action taken after the fact.
    pub(super) async fn supersede_note(
        &self,
        workflow_id: &str,
        note_id: &str,
        by: &str,
    ) -> Result<bool, WorkflowError> {
        self.update_journal(workflow_id, |notes| {
            let mut changed = false;
            for note in notes
                .iter_mut()
                .filter(|n| n.id == note_id && n.superseded_by.is_none())
            {
                note.superseded_by = Some(by.to_string());
                changed = true;
            }
            changed
        })
        .await
    }

    /// Applies `change` to the journal under compare-and-swap, writing it back
    /// only when `change` says it did something.
    async fn update_journal(
        &self,
        workflow_id: &str,
        change: impl Fn(&mut Vec<WorkflowNote>) -> bool,
    ) -> Result<bool, WorkflowError> {
        let id = doc_id(workflow_id)?;
        for _ in 0..CAS_ATTEMPTS {
            let (stored, mut notes) = self.journal(workflow_id).await?;
            if !change(&mut notes) {
                return Ok(false);
            }
            let notes_json = serde_json::to_string(&notes)
                .map_err(|error| malformed("serializing journal", error))?;
            let doc = json!({ "workflow_id": workflow_id, "notes_json": notes_json });
            let precondition = stored
                .as_ref()
                .map_or(Precondition::Absent, Versioned::unchanged);
            match self.docs.put(NOTES, &id, doc, precondition).await {
                Ok(_) => return Ok(true),
                Err(error) if is_race(error.kind()) => {}
                Err(error) => return Err(storage(error)),
            }
        }
        Err(WorkflowError::Storage(format!(
            "the journal of '{workflow_id}' kept changing under {CAS_ATTEMPTS} attempts"
        )))
    }
}
