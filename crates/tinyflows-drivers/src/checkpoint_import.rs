//! Import-only operations that preserve data already written by live runs.

use super::*;

impl<State> DriverCheckpointer<State> {
    /// Adds pending writes from a legacy import only when their task/index
    /// identity is absent, preserving every value already held by a live run.
    ///
    /// Unlike [`Checkpointer::put_writes`], imported control-plane writes do
    /// not replace an existing value. The repo update is a versioned CAS, so a
    /// concurrent writer is merged before the import retries.
    pub async fn import_writes_if_absent(
        &self,
        config: &CheckpointConfig,
        incoming: &[PendingWrite],
    ) -> Result<(usize, usize)> {
        let checkpoint_id = require_checkpoint_id(config)?;
        if incoming.is_empty() {
            return Ok((0, 0));
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
                });
                for write in incoming {
                    if body
                        .writes
                        .iter()
                        .any(|stored| stored.identity() == write.identity())
                    {
                        existing += 1;
                    } else {
                        body.writes.push(write.clone());
                        written += 1;
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
