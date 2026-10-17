//! Cursor-addressable run events stored atomically on the run row.

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use tinystoragedrivers_core::DocumentStoreExt;

use super::{FlowCatalogDocuments, RUNS, compare_and_swap, is_conflict};

const EVENT_ATTEMPTS: usize = 128;

impl FlowCatalogDocuments {
    /// Removes only the untouched initial row from an admission attempt that
    /// failed before its execution snapshot or events were persisted.
    pub async fn remove_unstarted_flow_run(&self, id: &str) -> Result<bool> {
        let docs = self.docs().await?;
        let Some(stored) = docs.get(RUNS, id).await? else {
            return Ok(false);
        };
        if stored.doc.get("status").and_then(Value::as_str) != Some("running")
            || stored.doc.get("execution_snapshot").is_some()
            || stored.doc.get("events_json").is_some()
        {
            return Ok(false);
        }
        docs.delete(RUNS, id, stored.unchanged())
            .await
            .map_err(Into::into)
    }

    /// Commits the terminal cursor only after the event itself is durable.
    pub async fn mark_flow_run_terminal_event(&self, id: &str, seq: u64) -> Result<bool> {
        let marked = compare_and_swap(self.docs().await?, RUNS, id, |doc| {
            if !matches!(
                doc.get("status").and_then(Value::as_str),
                Some(
                    "completed"
                        | "completed_with_warnings"
                        | "failed"
                        | "cancelled"
                        | "interrupted"
                )
            ) {
                return None;
            }
            let events: Vec<Value> = serde_json::from_str(
                doc.get("events_json")
                    .and_then(Value::as_str)
                    .unwrap_or("[]"),
            )
            .ok()?;
            let last = events.last().and_then(|event| event["seq"].as_u64())?;
            if last < seq {
                return None;
            }
            if let Some(marked) = doc.get("terminal_event_seq").and_then(Value::as_u64) {
                return (marked == seq).then(|| doc.clone());
            }
            let mut next = doc.clone();
            next["terminal_event_seq"] = json!(seq);
            Some(next)
        })
        .await?;
        Ok(marked.is_some())
    }

    /// The durable cursor committed after the terminal run event.
    pub async fn flow_run_terminal_event_seq(&self, id: &str) -> Result<Option<u64>> {
        let stored = self
            .docs()
            .await?
            .get(RUNS, id)
            .await?
            .context("flow run does not exist")?;
        Ok(stored.doc.get("terminal_event_seq").and_then(Value::as_u64))
    }

    /// Stores the exact execution snapshot used to start a run. Parked-run
    /// resume and cancellation must use this snapshot rather than a newer
    /// saved definition.
    pub async fn set_flow_run_snapshot(&self, id: &str, snapshot: Value) -> Result<()> {
        let updated = compare_and_swap(self.docs().await?, RUNS, id, |doc| {
            if doc.get("status").and_then(Value::as_str) != Some("running") {
                return None;
            }
            let mut next = doc.clone();
            next["execution_snapshot"] = snapshot.clone();
            Some(next)
        })
        .await?;
        if updated.is_none() {
            bail!("flow run is missing or no longer running")
        }
        Ok(())
    }

    /// Reads the exact snapshot pinned before a run actor was launched.
    pub async fn flow_run_snapshot(&self, id: &str) -> Result<Option<Value>> {
        Ok(self
            .docs()
            .await?
            .get(RUNS, id)
            .await?
            .and_then(|stored| stored.doc.get("execution_snapshot").cloned()))
    }

    /// Appends an event to a run's event stream using the run document CAS.
    /// Sequence allocation belongs to the run owner; this operation rejects
    /// duplicate or out-of-order positions so retries cannot fork the stream.
    pub async fn append_flow_run_event(&self, id: &str, event: Value) -> Result<()> {
        let seq = event
            .get("seq")
            .and_then(Value::as_u64)
            .context("run event has no unsigned sequence")?;
        for _ in 0..EVENT_ATTEMPTS {
            let append = compare_and_swap(self.docs().await?, RUNS, id, |doc| {
                let mut events: Vec<Value> = serde_json::from_str(
                    doc.get("events_json")
                        .and_then(Value::as_str)
                        .unwrap_or("[]"),
                )
                .ok()?;
                let last = events
                    .last()
                    .and_then(|event| event["seq"].as_u64())
                    .unwrap_or(0);
                if seq != last.checked_add(1)? {
                    return None;
                }
                events.push(event.clone());
                let mut next = doc.clone();
                next["events_json"] = json!(serde_json::to_string(&events).ok()?);
                Some(next)
            })
            .await;
            match append {
                Ok(Some(_)) => return Ok(()),
                Ok(None) => {
                    let stored = self
                        .docs()
                        .await?
                        .get(RUNS, id)
                        .await?
                        .context("flow run does not exist")?;
                    let events: Vec<Value> = serde_json::from_str(
                        stored
                            .doc
                            .get("events_json")
                            .and_then(Value::as_str)
                            .unwrap_or("[]"),
                    )
                    .context("flow run events are corrupt")?;
                    let last = events
                        .last()
                        .and_then(|event| event["seq"].as_u64())
                        .unwrap_or(0);
                    if seq <= last {
                        return Ok(());
                    }
                    bail!("run event sequence {seq} followed {last} with a gap");
                }
                Err(error) if is_conflict(&error) => continue,
                Err(error) => return Err(error).context("appending flow run event"),
            }
        }
        bail!("flow run {id} kept changing while appending an event")
    }

    /// Reads a bounded, strictly ordered page after `cursor`.
    pub async fn flow_run_events_after(
        &self,
        id: &str,
        cursor: u64,
        limit: usize,
    ) -> Result<Vec<Value>> {
        let stored = self
            .docs()
            .await?
            .get(RUNS, id)
            .await?
            .context("flow run does not exist")?;
        let events: Vec<Value> = serde_json::from_str(
            stored
                .doc
                .get("events_json")
                .and_then(Value::as_str)
                .unwrap_or("[]"),
        )
        .context("flow run events are corrupt")?;
        let mut page = Vec::new();
        let mut expected = cursor;
        for event in events.into_iter().filter(|event| {
            event
                .get("seq")
                .and_then(Value::as_u64)
                .is_some_and(|seq| seq > cursor)
        }) {
            let seq = event["seq"]
                .as_u64()
                .context("run event sequence is invalid")?;
            if seq != expected.saturating_add(1) {
                bail!(
                    "run event cursor gap: expected {}, received {seq}",
                    expected + 1
                );
            }
            expected = seq;
            page.push(event);
            if page.len() >= limit.max(1) {
                break;
            }
        }
        Ok(page)
    }

    /// Highest event sequence persisted for `id`, or zero when no event exists.
    pub async fn flow_run_event_last_seq(&self, id: &str) -> Result<u64> {
        let stored = self
            .docs()
            .await?
            .get(RUNS, id)
            .await?
            .context("flow run does not exist")?;
        let events: Vec<Value> = serde_json::from_str(
            stored
                .doc
                .get("events_json")
                .and_then(Value::as_str)
                .unwrap_or("[]"),
        )
        .context("flow run events are corrupt")?;
        Ok(events
            .last()
            .and_then(|event| event.get("seq").and_then(Value::as_u64))
            .unwrap_or(0))
    }
}

#[cfg(test)]
#[path = "run_events_tests.rs"]
mod tests;
