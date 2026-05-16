mod backend;

use std::path::{Path, PathBuf};

use crate::error::{DbError, DbResult};
use crate::schema::DatabaseSchema;

use self::backend::StorageBackend;

// Re-export for external consumers (engine, wal).
pub use self::backend::StoredRow;

cfg_select! {
    feature = "btree" => {
        pub use self::backend::BTreeBackend as Backend;
    }
    feature = "json" => {
        pub use self::backend::JsonBackend as Backend;
    }
    _ => {
        compile_error!("At least one storage backend feature must be enabled: 'btree' or 'json'");
    }
}

/// Default storage type: JSON backend.
pub type Storage = StorageImpl<Backend>;

/// Storage facade that wraps a [StorageBackend] with convenience helpers.
///
/// The backend handles format-specific I/O and path conventions.  The facade
/// exposes schema load/save, row I/O, and lower-level file helpers.
/// Atomic composite operations (`create_table`, `drop_table`, `rename_table`)
/// that need to coordinate schema + data file belong to the engine layer.
///
/// The storage layer does **not** perform schema-aware validation or constraint
/// enforcement — those belong to the engine.
pub struct StorageImpl<B: StorageBackend> {
    root: PathBuf,
    backend: B,
}

impl<B: StorageBackend + Default> StorageImpl<B> {
    // ── Construction ────────────────────────────────────────────────

    /// Create a storage handle rooted at `root`.
    pub fn new(root: impl AsRef<Path>) -> DbResult<Self> {
        let root = root.as_ref().to_path_buf();
        let backend = B::default();
        backend.create_dir_all(&root)?;
        Ok(Self { root, backend })
    }

    // ── Schema (schema file) ────────────────────────────────────────

    /// Load the schema metadata from disk.
    ///
    /// If the schema file does not exist yet, an empty default schema is
    /// returned.
    pub fn load_schema(&self) -> DbResult<DatabaseSchema> {
        let path = self.backend.schema_path(&self.root);
        if !self.backend.file_exists(&path) {
            return Ok(DatabaseSchema::new("default"));
        }
        self.backend.load_schema(&path)
    }

    /// Persist schema metadata to disk.
    pub fn save_schema(&self, schema: &DatabaseSchema) -> DbResult<()> {
        let path = self.backend.schema_path(&self.root);
        self.backend.save_schema(&path, schema)
    }

    // ── Table file helpers (engine uses these to build compound ops) ──

    /// Returns `true` if a table data file exists for `table`.
    pub(crate) fn table_file_exists(&self, table: &str) -> bool {
        let path = self.backend.table_path(&self.root, table);
        self.backend.file_exists(&path)
    }

    /// Create an empty table data file.
    pub(crate) fn create_table_file(&self, table: &str) -> DbResult<()> {
        let path = self.backend.table_path(&self.root, table);
        self.backend.create_file(&path)
    }

    /// Remove a table data file.
    ///
    /// Returns an error if the file does not exist.
    pub(crate) fn remove_table_file(&self, table: &str) -> DbResult<()> {
        let path = self.backend.table_path(&self.root, table);
        self.backend.remove_file(&path)
    }

    /// Atomically rename a table data file from `old_name` to `new_name`.
    pub(crate) fn rename_table_file(&self, old_name: &str, new_name: &str) -> DbResult<()> {
        let old_path = self.backend.table_path(&self.root, old_name);
        let new_path = self.backend.table_path(&self.root, new_name);
        self.backend.rename_file(&old_path, &new_path)
    }

    // ── Row I/O (table data files) ──────────────────────────────────

    /// Ensure a table data file exists.
    pub(crate) fn ensure_table_exists(&self, table: &str) -> DbResult<()> {
        let table_path = self.backend.table_path(&self.root, table);
        if !self.backend.file_exists(&table_path) {
            return Err(DbError::TableNotFound(table.to_string()));
        }
        Ok(())
    }

    /// Load all stored rows from a table in file order.
    pub fn load_rows(&self, table: &str) -> DbResult<Vec<StoredRow>> {
        let mut rows = Vec::new();
        self.scan_apply_rows(table, |row| {
            rows.push(row.clone());
            Ok(())
        })?;
        Ok(rows)
    }

    /// Scan all stored rows from a table and apply a callback to each.
    pub fn scan_apply_rows<F>(&self, table: &str, mut func: F) -> DbResult<()>
    where
        F: FnMut(&StoredRow) -> DbResult<()>,
    {
        self.ensure_table_exists(table)?;
        let path = self.backend.table_path(&self.root, table);
        self.backend.scan_rows(&path, |row| func(row))
    }

    /// Append one already-prepared stored row to a table file.
    pub fn append_stored_row(&self, table: &str, row: &StoredRow) -> DbResult<()> {
        self.ensure_table_exists(table)?;
        let path = self.backend.table_path(&self.root, table);
        self.backend.append_row(&path, row)
    }

    /// Atomically rewrite a table file with the provided stored rows.
    pub fn rewrite_rows(&self, table: &str, rows: &[StoredRow]) -> DbResult<()> {
        self.ensure_table_exists(table)?;
        let path = self.backend.table_path(&self.root, table);
        self.backend.rewrite_rows(&path, rows)
    }

    /// Rewrite a table file even if it doesn't exist yet (WAL recovery).
    pub(crate) fn force_rewrite_rows(&self, table: &str, rows: &[StoredRow]) -> DbResult<()> {
        let path = self.backend.table_path(&self.root, table);
        self.backend.rewrite_rows(&path, rows)
    }

    // ── Path helpers ─────────────────────────────────────────────────

    pub(crate) fn wal_path(&self) -> PathBuf {
        self.backend.wal_path(&self.root)
    }

    pub(crate) fn table_path(&self, table: &str) -> PathBuf {
        self.backend.table_path(&self.root, table)
    }
}
