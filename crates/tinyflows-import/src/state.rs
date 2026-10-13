//! The retired `FileStateStore`: one JSON file per key.
//!
//! A key was hashed into its file name, so the files cannot be listed back into
//! keys: the old store keeps no index. The caller names the keys it wants (a
//! host knows which state keys its flows use), and each is looked up by the
//! same hash the old store used.

use std::path::Path;

use anyhow::{Context, Result};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tinyflows_drivers::catalog::FlowCatalogDocuments;

use crate::report::ImportReport;

/// The values stored under `keys` in `namespace` beneath `root`, the directory
/// a `FileStateStore` was created with, copied into `catalog`'s state under the
/// same namespace and key (where `FlowStateDocuments` reads them). A key that
/// has no file is not an error and is not counted.
///
/// # Errors
///
/// When a state file exists but cannot be read, or the target store fails. A
/// file that is not JSON is counted in the report as invalid.
pub async fn import_file_state(
    root: &Path,
    namespace: &str,
    keys: &[String],
    catalog: &FlowCatalogDocuments,
) -> Result<ImportReport> {
    let mut report = ImportReport::default();
    let dir = root.join(digest(namespace));
    for key in keys {
        let path = dir.join(format!("{}.json", digest(key)));
        let body = match std::fs::read(&path) {
            Ok(body) => body,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error).with_context(|| format!("reading {}", path.display())),
        };
        match serde_json::from_slice::<Value>(&body) {
            Ok(value) => report.wrote("state", catalog.import_kv(namespace, key, &value).await?),
            Err(error) => report.invalid("state", format!("{namespace}/{key}: {error}")),
        }
    }
    Ok(report)
}

/// The hex SHA-256 the old store turned a namespace or key into a path
/// component with.
fn digest(value: &str) -> String {
    hex::encode(Sha256::digest(value.as_bytes()))
}

#[cfg(test)]
#[path = "state_tests.rs"]
mod tests;
