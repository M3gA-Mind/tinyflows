//! Fixtures the import tests share.
//!
//! `tests/fixtures` holds one store of every legacy kind, written by the
//! retired writers (`tinyflows-sqlite`, `FileCheckpointer`, `FileStateStore`,
//! `FileWorkflowStore`), and `expected.json`, the same data read back through
//! those stores' own readers. The committed files are never written to: tests
//! that import from them also check that no file under the tree changed.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde_json::Value;
use sha2::{Digest, Sha256};
use tinystoragedrivers_core::{DocumentStore, MemoryStorage, Scope, StorageBackend};

/// The committed legacy stores.
pub(crate) fn fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

/// What the legacy readers saw in [`fixtures`].
pub(crate) fn expected() -> Value {
    let body = std::fs::read(fixtures().join("expected.json")).expect("expected.json");
    canonical(serde_json::from_slice(&body).expect("expected.json parses"))
}

/// `value` with every RFC 3339 string rewritten to millisecond precision in
/// UTC: storage keeps one epoch-millisecond field per instant, so a
/// comparison with the legacy fixtures is made at that precision.
pub(crate) fn canonical(value: Value) -> Value {
    match value {
        Value::String(text) => match chrono::DateTime::parse_from_rfc3339(&text) {
            Ok(at) => Value::String(
                at.with_timezone(&chrono::Utc)
                    .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            ),
            Err(_) => Value::String(text),
        },
        Value::Array(items) => Value::Array(items.into_iter().map(canonical).collect()),
        Value::Object(map) => {
            Value::Object(map.into_iter().map(|(k, v)| (k, canonical(v))).collect())
        }
        other => other,
    }
}

/// A document handle under `scope` of a fresh in-memory backend.
pub(crate) fn docs() -> Arc<dyn DocumentStore> {
    docs_in(&MemoryStorage::new(), "local")
}

/// `storage`'s documents under `scope`.
pub(crate) fn docs_in(storage: &MemoryStorage, scope: &str) -> Arc<dyn DocumentStore> {
    Arc::clone(
        storage
            .for_scope(&Scope::new(scope).unwrap())
            .unwrap()
            .documents(),
    )
}

/// Every file under `dir` and the digest of its bytes, by relative path.
pub(crate) fn tree_digest(dir: &Path) -> BTreeMap<String, String> {
    let mut digests = BTreeMap::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(next) = pending.pop() {
        for entry in std::fs::read_dir(&next).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path);
            } else {
                let relative = path
                    .strip_prefix(dir)
                    .unwrap()
                    .to_string_lossy()
                    .into_owned();
                digests.insert(
                    relative,
                    hex::encode(Sha256::digest(std::fs::read(&path).unwrap())),
                );
            }
        }
    }
    digests
}

/// The JSON form, for the model types that are not `PartialEq`.
pub(crate) fn json<T: serde::Serialize>(value: &T) -> Value {
    canonical(serde_json::to_value(value).unwrap())
}

/// `items` sorted by their `key` string, for comparing lists whose order the
/// two stores define differently.
#[cfg(feature = "catalog")]
pub(crate) fn sorted_by(mut items: Vec<Value>, key: &str) -> Vec<Value> {
    items.sort_by(|a, b| a[key].as_str().cmp(&b[key].as_str()));
    items
}
