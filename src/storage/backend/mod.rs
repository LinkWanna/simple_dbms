//! Pluggable storage backends.
//!
//! Each backend defines file naming conventions (extensions) and the actual
//! serialization format for schema metadata and table row data.

use std::path::{Path, PathBuf};

use crate::error::DbResult;
use crate::schema::{DatabaseSchema, Value};

/// Row persisted on disk.
///
/// Each user-visible row is stored together with an internal `row_id` so the
/// engine can implement row-oriented undo logging without exposing this
/// identifier at the SQL layer.
///
/// This type is shared across all backends — the logical row structure (row_id
/// + values) is format-agnostic. Only the serialization format differs per
/// backend.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct StoredRow {
    pub row_id: u64,
    pub values: Vec<Value>,
}

/// Abstraction over how schema metadata and table row data are persisted.
///
/// A backend owns both the **naming convention** (file extensions, paths) and
/// the **wire format** (JSON, binary, etc.). Swapping the backend is enough to
/// change the entire on-disk representation.
pub trait StorageBackend {
    // ── Paths (layout) ───────────────────────────────────────────

    /// Absolute path to the schema metadata file under `root`.
    fn schema_path(&self, root: &Path) -> PathBuf;

    /// Absolute path to the write-ahead log under `root`.
    fn wal_path(&self, root: &Path) -> PathBuf;

    /// Absolute path to the table data file for `table` under `root`.
    fn table_path(&self, root: &Path, table: &str) -> PathBuf;

    // ── Schema I/O ───────────────────────────────────────────────

    /// Deserialize schema metadata from the given file.
    ///
    /// Returns an error if the file does not exist or cannot be decoded.
    fn load_schema(&self, path: &Path) -> DbResult<DatabaseSchema>;

    /// Serialize schema metadata to the given file (overwrites).
    fn save_schema(&self, path: &Path, schema: &DatabaseSchema) -> DbResult<()>;

    // ── Row I/O ──────────────────────────────────────────────────

    /// Scan all rows from a table file, calling `func` for each.
    fn scan_rows<F>(&self, path: &Path, func: F) -> DbResult<()>
    where
        F: FnMut(&StoredRow) -> DbResult<()>;

    /// Append a single row to a table file.
    fn append_row(&self, path: &Path, row: &StoredRow) -> DbResult<()>;

    /// Atomically rewrite a table file with the given rows.
    ///
    /// The implementation should use a temporary file + rename to avoid
    /// partial writes on crash.
    fn rewrite_rows(&self, path: &Path, rows: &[StoredRow]) -> DbResult<()>;

    // ── File-system helpers ──────────────────────────────────────

    /// Create an empty regular file at `path`.
    fn create_file(&self, path: &Path) -> DbResult<()>;

    /// Remove the file at `path` if it exists; error otherwise.
    fn remove_file(&self, path: &Path) -> DbResult<()>;

    /// Atomically rename `from` to `to`.
    fn rename_file(&self, from: &Path, to: &Path) -> DbResult<()>;

    /// Returns `true` if a file exists at `path`.
    fn file_exists(&self, path: &Path) -> bool;

    /// Recursively create the directory containing `path`.
    fn create_dir_all(&self, path: &Path) -> DbResult<()>;
}

// ── JSON backend ──────────────────────────────────────────────────────

mod json;
pub use json::JsonBackend;
