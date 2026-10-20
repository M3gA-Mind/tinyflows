//! Import-only operations that preserve data already written by live runs.

use super::*;

use tinyflows::graph::checkpoint::merge_writes;

/// Stores live pending writes, relinquishing import provenance for every
/// accepted control-plane identity, including an idempotent same-value write.
pub(super) async fn put_live_writes<State>(
    checkpointer: &DriverCheckpointer<State>,
    config: &CheckpointConfig,
    writes: &[PendingWrite],
) -> Result<()> {
    let checkpoint_id = require_checkpoint_id(config)?;
    if writes.is_empty() {
        return Ok(());
    }
    checkpointer.declared().await?;
    let subgraph = subgraph_key(&config.namespace);
    let id = writes_id_key(
        &checkpointer.ns,
        &config.thread_id,
        &subgraph,
        &checkpoint_id,
    );
    checkpointer
        .writes
        .update(&id, |current| {
            let mut body = current.unwrap_or_else(|| WritesBody {
                ns: checkpointer.ns.clone(),
                thread: config.thread_id.clone(),
                subgraph: subgraph.clone(),
                checkpoint_id: checkpoint_id.clone(),
                writes: Vec::new(),
                imported_control_writes: Default::default(),
            });
            for write in writes {
                let identity = (write.task_id.clone(), write.idx);
                let accepted = write.is_control_plane()
                    || !body
                        .writes
                        .iter()
                        .any(|stored| stored.identity() == write.identity());
                if accepted {
                    body.imported_control_writes.remove(&identity);
                }
            }
            merge_writes(&mut body.writes, writes);
            Ok(Some(body))
        })
        .await
        .map(|_| ())
        .map_err(|error| {
            if error.kind() == ErrorKind::Conflict {
                GraphError::Checkpoint(format!(
                    "could not record pending writes for thread \u{60}{}\u{60}",
                    config.thread_id
                ))
            } else {
                map_error(error)
            }
        })
}

impl<State> DriverCheckpointer<State> {
    /// Imports ordered pending writes while preserving target-owned values.
    ///
    /// Control-plane records may advance a value still marked as imported,
    /// including across bounded import batches. The stored provenance is
    /// cleared when [`Checkpointer::put_writes`] accepts a live control write,
    /// so that value wins on a concurrent retry. Data writes remain
    /// insert-once. Older write documents have no import markers and remain
    /// protected as target-owned data.
    pub async fn import_writes_if_absent(
        &self,
        config: &CheckpointConfig,
        incoming: &[PendingWrite],
    ) -> Result<(usize, usize)> {
        let checkpoint_id = require_checkpoint_id(config)?;
        if incoming.is_empty() {
            return Ok((0, 0));
        }
        let mut grouped: Vec<(PendingWrite, usize)> = Vec::new();
        for write in incoming {
            if let Some((last, count)) = grouped
                .iter_mut()
                .find(|(known, _)| known.identity() == write.identity())
            {
                *count += 1;
                if write.is_control_plane() {
                    *last = write.clone();
                }
            } else {
                grouped.push((write.clone(), 1));
            }
        }
        self.declared().await?;
        let subgraph = subgraph_key(&config.namespace);
        let id = writes_id_key(&self.ns, &config.thread_id, &subgraph, &checkpoint_id);
        let (mut written, mut existing) = (0, 0);
        self.writes
            .update(&id, |current| {
                written = 0;
                existing = 0;
                let mut body = current.unwrap_or_else(|| WritesBody {
                    ns: self.ns.clone(),
                    thread: config.thread_id.clone(),
                    subgraph: subgraph.clone(),
                    checkpoint_id: checkpoint_id.clone(),
                    writes: Vec::new(),
                    imported_control_writes: Default::default(),
                });
                for (write, count) in &grouped {
                    let identity = (write.task_id.clone(), write.idx);
                    let held = body
                        .writes
                        .iter()
                        .position(|stored| stored.identity() == write.identity());
                    match held {
                        Some(index)
                            if write.is_control_plane()
                                && body.imported_control_writes.contains(&identity) =>
                        {
                            if body.writes[index] == *write {
                                existing += count;
                            } else {
                                body.writes[index] = write.clone();
                                written += count;
                            }
                        }
                        Some(_) => existing += count,
                        None if write.is_control_plane() => {
                            body.writes.push(write.clone());
                            body.imported_control_writes.insert(identity);
                            written += count;
                        }
                        None => {
                            body.writes.push(write.clone());
                            written += 1;
                            existing += count - 1;
                        }
                    }
                }
                Ok((written > 0).then_some(body))
            })
            .await
            .map_err(map_error)?;
        Ok((written, existing))
    }
}

#[cfg(test)]
#[path = "checkpoint_import_tests.rs"]
mod tests;
