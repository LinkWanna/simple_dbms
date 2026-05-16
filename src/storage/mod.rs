mod backend;

use std::path::{Path, PathBuf};

use crate::error::{DbError, DbResult};
use crate::schema::{DatabaseSchema, TableSchema};

use self::backend::{JsonBackend, StorageBackend};

// Re-export for external consumers (engine, wal).
pub use self::backend::StoredRow;

/// Default storage type: JSON backend.
pub type Storage = StorageImpl<JsonBackend>;

/// Storage facade that composes a [StorageBackend] with coordination logic.
///
/// The backend handles format-specific I/O and path conventions.  The facade
/// adds atomic composite operations (`create_table`, `drop_table`,
/// `rename_table`) that keep the schema entry and data file in sync, plus
/// convenience helpers for the engine.
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

    // ── Compound table operations (file + schema atomically) ─────────

    /// Create a new table: empty data file + schema entry, atomically.
    pub fn create_table(&self, table: TableSchema) -> DbResult<()> {
        let table_path = self.backend.table_path(&self.root, &table.name);
        if self.backend.file_exists(&table_path) {
            return Err(DbError::TableExists(table.name));
        }

        self.backend.create_file(&table_path)?;

        let schema_result = {
            let mut schema = self.load_schema()?;
            if schema.tables.contains_key(&table.name) {
                return Err(DbError::TableExists(table.name.clone()));
            }
            schema.add_table(table.clone())?;
            self.save_schema(&schema)
        };

        if let Err(error) = schema_result {
            let _ = self.backend.remove_file(&table_path);
            return Err(error);
        }

        Ok(())
    }

    /// Drop an existing table: data file + schema entry, atomically.
    pub fn drop_table(&self, table: &str, if_exists: bool) -> DbResult<()> {
        let table_path = self.backend.table_path(&self.root, table);
        if self.backend.file_exists(&table_path) {
            self.backend.remove_file(&table_path)?;
        } else if !if_exists {
            return Err(DbError::TableNotFound(table.to_string()));
        }

        let mut schema = self.load_schema()?;
        if schema.tables.remove(table).is_none() {
            if if_exists {
                return Ok(());
            }
            return Err(DbError::TableNotFound(table.to_string()));
        }
        self.save_schema(&schema)
    }

    /// Rename an existing table: data file + schema entry, atomically.
    pub fn rename_table(&self, old_name: &str, new_name: &str) -> DbResult<()> {
        let old_path = self.backend.table_path(&self.root, old_name);
        if !self.backend.file_exists(&old_path) {
            return Err(DbError::TableNotFound(old_name.to_string()));
        }

        let new_path = self.backend.table_path(&self.root, new_name);
        if self.backend.file_exists(&new_path) {
            return Err(DbError::TableExists(new_name.to_string()));
        }

        self.backend.rename_file(&old_path, &new_path)?;

        let schema_result = {
            let mut schema = self.load_schema()?;
            if schema.tables.contains_key(new_name) {
                return Err(DbError::TableExists(new_name.to_string()));
            }
            let mut table_schema = schema
                .tables
                .remove(old_name)
                .ok_or_else(|| DbError::TableNotFound(old_name.to_string()))?;
            table_schema.name = new_name.to_string();
            schema.tables.insert(new_name.to_string(), table_schema);
            self.save_schema(&schema)
        };

        if let Err(error) = schema_result {
            let _ = self.backend.rename_file(&new_path, &old_path);
            return Err(error);
        }

        Ok(())
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

    // ── Path helpers ─────────────────────────────────────────────────

    pub(crate) fn wal_path(&self) -> PathBuf {
        self.backend.wal_path(&self.root)
    }

    pub(crate) fn table_path(&self, table: &str) -> PathBuf {
        self.backend.table_path(&self.root, table)
    }
}
