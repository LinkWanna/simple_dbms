pub mod layout;

use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::{DbError, DbResult};
use crate::schema::{DatabaseSchema, TableSchema, Value};

use self::layout::StorageLayout;

/// Row persisted on disk.
///
/// Each user-visible row is stored together with an internal `row_id` so the
/// engine can implement row-oriented undo logging without exposing this
/// identifier at the SQL layer.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StoredRow {
    pub row_id: u64,
    pub values: Vec<Value>,
}

/// Storage handle coordinating all file-based persistence for the DBMS.
///
/// Responsibilities (physical invariants only):
/// - Filesystem layout via `StorageLayout`
/// - `schema.json` read/write
/// - Table JSONL file CRUD (rows as `StoredRow`)
/// - Atomic composite operations (`create_table`, `drop_table`, `rename_table`)
///   that keep the schema entry and data file in sync
///
/// The storage layer does NOT perform schema-aware validation or constraint
/// enforcement — those belong to the engine layer.
pub struct Storage {
    layout: StorageLayout,
}

impl Storage {
    // ── Construction ────────────────────────────────────────────────

    /// Create a storage handle rooted at the given directory.
    pub fn new(root: impl AsRef<Path>) -> DbResult<Self> {
        let layout = StorageLayout::new(root);
        fs::create_dir_all(layout.root())?;
        Ok(Self { layout })
    }

    /// Load the schema metadata from disk.
    ///
    /// If the schema file does not exist yet, an empty default schema is
    /// returned.
    pub fn load_schema(&self) -> DbResult<DatabaseSchema> {
        let path = self.layout.schema_path();
        if !path.exists() {
            return Ok(DatabaseSchema::new("default"));
        }

        let file = File::open(path)?;
        let reader = BufReader::new(file);
        Ok(serde_json::from_reader(reader)?)
    }

    /// Persist schema metadata to disk.
    pub fn save_schema(&self, schema: &DatabaseSchema) -> DbResult<()> {
        let file = File::create(self.layout.schema_path())?;
        serde_json::to_writer_pretty(file, schema)?;
        Ok(())
    }

    /// Add a new table definition to schema metadata (schema.json only).
    ///
    /// The caller is responsible for creating the table data file before
    /// calling this, or using [`create_table`] for the atomic composite.
    fn create_table_schema(&self, table: TableSchema) -> DbResult<()> {
        let mut schema = self.load_schema()?;
        if schema.tables.contains_key(&table.name) {
            return Err(DbError::TableExists(table.name.clone()));
        }
        schema.add_table(table)?;
        self.save_schema(&schema)
    }

    /// Remove a table definition from schema metadata (schema.json only).
    fn drop_table_schema(&self, table: &str, if_exists: bool) -> DbResult<()> {
        let mut schema = self.load_schema()?;
        if schema.tables.remove(table).is_none() {
            if if_exists {
                return Ok(());
            }
            return Err(DbError::TableNotFound(table.to_string()));
        }
        self.save_schema(&schema)
    }

    /// Rename a table definition inside schema metadata.
    fn rename_table_schema(&self, old_name: &str, new_name: &str) -> DbResult<()> {
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
    }

    // ── Compound table operations (file + schema atomically) ─────────

    /// Create a new table: empty data file + schema entry, atomically.
    ///
    /// The data file is created first. If the schema update fails, the file
    /// is removed so the two stay consistent.
    pub fn create_table(&self, table: TableSchema) -> DbResult<()> {
        let table_path = self.layout.table_path(&table.name);
        if table_path.exists() {
            return Err(DbError::TableExists(table.name));
        }

        File::create(&table_path)?;

        if let Err(error) = self.create_table_schema(table.clone()) {
            let _ = fs::remove_file(&table_path);
            return Err(error);
        }

        Ok(())
    }

    /// Drop an existing table: data file + schema entry, atomically.
    ///
    /// The data file is removed first. Schema removal follows so that
    /// metadata-only drops don't happen when file deletion fails.
    pub fn drop_table(&self, table: &str, if_exists: bool) -> DbResult<()> {
        let table_path = self.layout.table_path(table);
        if table_path.exists() {
            fs::remove_file(&table_path)?;
        } else if !if_exists {
            return Err(DbError::TableNotFound(table.to_string()));
        }

        self.drop_table_schema(table, if_exists)
    }

    /// Rename an existing table: data file + schema entry, atomically.
    ///
    /// The file is renamed first. If the schema update fails, the rename is
    /// reversed so the two stay consistent.
    pub fn rename_table(&self, old_name: &str, new_name: &str) -> DbResult<()> {
        let old_path = self.layout.table_path(old_name);
        if !old_path.exists() {
            return Err(DbError::TableNotFound(old_name.to_string()));
        }

        let new_path = self.layout.table_path(new_name);
        if new_path.exists() {
            return Err(DbError::TableExists(new_name.to_string()));
        }

        fs::rename(&old_path, &new_path)?;

        if let Err(error) = self.rename_table_schema(old_name, new_name) {
            let _ = fs::rename(&new_path, &old_path);
            return Err(error);
        }

        Ok(())
    }

    // ── Row I/O (JSONL table files) ─────────────────────────────────

    /// Ensure a table data file exists.
    pub(crate) fn ensure_table_exists(&self, table: &str) -> DbResult<()> {
        let table_path = self.layout.table_path(table);
        if !table_path.exists() {
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

    /// Scan all stored rows from a table and apply a callback to each row.
    pub fn scan_apply_rows<F>(&self, table: &str, mut func: F) -> DbResult<()>
    where
        F: FnMut(&StoredRow) -> DbResult<()>,
    {
        self.ensure_table_exists(table)?;

        let file = File::open(self.layout.table_path(table))?;
        let reader = BufReader::new(file);

        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let row: StoredRow = serde_json::from_str(&line)?;
            func(&row)?;
        }

        Ok(())
    }

    /// Append one already-prepared stored row to a table file.
    ///
    /// This is a low-level persistence primitive. The caller is responsible
    /// for schema validation, constraint checks, and row-id allocation.
    pub fn append_stored_row(&self, table: &str, row: &StoredRow) -> DbResult<()> {
        self.ensure_table_exists(table)?;

        let mut file = OpenOptions::new()
            .create(false)
            .append(true)
            .open(self.layout.table_path(table))?;
        let line = serde_json::to_string(row)?;
        file.write_all(line.as_bytes())?;
        file.write_all(b"\n")?;
        Ok(())
    }

    /// Rewrite a table file with the provided stored rows.
    ///
    /// Uses a temporary file + atomic rename to avoid partial writes.
    /// The caller is responsible for ensuring the provided rows are valid
    /// against the table schema and constraints.
    pub fn rewrite_rows(&self, table: &str, rows: &[StoredRow]) -> DbResult<()> {
        self.ensure_table_exists(table)?;

        let table_path = self.layout.table_path(table);
        let temp_path = self.layout.temp_table_path(table);
        {
            let mut file = File::create(&temp_path)?;
            for row in rows {
                let line = serde_json::to_string(row)?;
                file.write_all(line.as_bytes())?;
                file.write_all(b"\n")?;
            }
            file.flush()?;
        }

        fs::rename(temp_path, table_path)?;
        Ok(())
    }

    // ── Path helpers ─────────────────────────────────────────────────

    pub(crate) fn wal_path(&self) -> std::path::PathBuf {
        self.layout.wal_path()
    }

    pub(crate) fn table_path(&self, table: &str) -> std::path::PathBuf {
        self.layout.table_path(table)
    }
}
