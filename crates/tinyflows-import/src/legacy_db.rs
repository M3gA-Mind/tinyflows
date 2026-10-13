//! Reading a legacy SQLite file without touching it.
//!
//! The file is copied, with its `-wal` and `-shm` if it has them, to a scratch
//! directory, and the copy is opened through the SQLite driver's native handle.
//! The driver creates what it needs and sets WAL mode on open, so opening the
//! original would modify it; the copy takes that instead and is deleted when
//! the [`LegacyDb`] drops.

use std::collections::HashSet;
use std::path::Path;

use anyhow::{Context, Result};
use tinystoragedrivers_sqlite::SqliteNative;
use tinystoragedrivers_sqlite::rusqlite::Connection;

/// A scratch copy of a legacy SQLite database, open for reading.
pub(crate) struct LegacyDb {
    // Held for its drop: the directory goes when the handle does.
    _scratch: tempfile::TempDir,
    native: SqliteNative,
}

impl LegacyDb {
    /// Opens a copy of the database at `path`; `None` when there is no such
    /// file, which is the normal state of a store that was never used.
    pub(crate) fn open(path: &Path) -> Result<Option<Self>> {
        if !path.is_file() {
            return Ok(None);
        }
        let scratch = tempfile::tempdir().context("creating a scratch directory")?;
        let name = path
            .file_name()
            .context("a database path has a file name")?
            .to_os_string();
        for suffix in ["", "-wal", "-shm"] {
            let mut from = path.as_os_str().to_os_string();
            from.push(suffix);
            let from = std::path::PathBuf::from(from);
            if from.is_file() {
                let mut to = name.clone();
                to.push(suffix);
                std::fs::copy(&from, scratch.path().join(to))
                    .with_context(|| format!("copying {}", from.display()))?;
            }
        }
        let native = SqliteNative::open(scratch.path().join(&name))
            .with_context(|| format!("opening a copy of {}", path.display()))?;
        Ok(Some(Self {
            _scratch: scratch,
            native,
        }))
    }

    /// Runs `read` on the copy's connection.
    pub(crate) async fn read<T, F>(&self, what: &'static str, read: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> tinystoragedrivers_sqlite::rusqlite::Result<T> + Send + 'static,
    {
        self.native
            .with_connection(read)
            .await
            .with_context(|| format!("reading {what} from the legacy database"))
    }

    /// The tables the database has.
    pub(crate) async fn tables(&self) -> Result<HashSet<String>> {
        self.read("the table list", |conn| {
            let mut statement =
                conn.prepare("SELECT name FROM sqlite_master WHERE type = 'table'")?;
            let names = statement
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<Result<HashSet<_>, _>>()?;
            Ok(names)
        })
        .await
    }

    /// The columns `table` has; empty when it has none or does not exist.
    pub(crate) async fn columns(&self, table: &'static str) -> Result<HashSet<String>> {
        self.read("a column list", move |conn| {
            let mut statement = conn.prepare(&format!("PRAGMA table_info({table})"))?;
            let names = statement
                .query_map([], |row| row.get::<_, String>(1))?
                .collect::<Result<HashSet<_>, _>>()?;
            Ok(names)
        })
        .await
    }
}

/// `column` when `have` lists it, else the literal `default`, as a select-list
/// entry: how a database written before a column existed is read as if the
/// column held its default.
pub(crate) fn column_or(have: &HashSet<String>, column: &str, default: &str) -> String {
    if have.contains(column) {
        column.to_string()
    } else {
        format!("{default} AS {column}")
    }
}

#[cfg(test)]
#[path = "legacy_db_tests.rs"]
mod tests;
