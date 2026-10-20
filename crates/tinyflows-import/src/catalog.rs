//! The public legacy catalog API, composed from the raw host reader and the
//! shared module-side semantic writer.

use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use tinyflows_drivers::catalog::FlowCatalogDocuments;

use crate::report::ImportReport;

/// Imports the retired `flows.db` and `drafts/*.json` sources into `catalog`.
///
/// The source reader emits bounded raw DTO batches. The same semantic writer
/// used by Catalog v6 performs graph migration, validation, idempotent writes
/// and report accounting, preserving the behavior of the established API.
pub async fn import_flow_catalog(
    dir: &Path,
    catalog: &FlowCatalogDocuments,
) -> Result<ImportReport> {
    let report = Arc::new(Mutex::new(ImportReport::default()));
    let report_for_send = Arc::clone(&report);
    let mut send = move |records: Vec<tinyflows_bus::catalog::LegacyImportRecord>| {
        let report = Arc::clone(&report_for_send);
        async move {
            let response = crate::legacy_batch::apply_legacy_batch(&records, catalog, None).await?;
            report
                .lock()
                .map_err(|_| anyhow::anyhow!("legacy import report lock poisoned"))?
                .merge_legacy_batch(response);
            Ok(())
        }
    };
    crate::flow_reader::read_legacy_flow_catalog(dir, &mut send)
        .await
        .with_context(|| format!("importing legacy flow catalog from {}", dir.display()))?;
    drop(send);
    Arc::try_unwrap(report)
        .map_err(|_| anyhow::anyhow!("legacy import report still has active readers"))?
        .into_inner()
        .map_err(|_| anyhow::anyhow!("legacy import report lock poisoned"))
}

#[cfg(test)]
#[path = "catalog_tests.rs"]
mod tests;
