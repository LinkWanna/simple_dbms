pub mod constraints;
pub mod layout;
pub mod row_store;
pub mod schema_store;

use std::path::Path;

use crate::error::{DbError, DbResult};
use crate::schema::{DatabaseSchema, TableSchema, Value};

use self::layout::StorageLayout;
use self::row_store::RowStore;
use self::schema_store::SchemaStore;

pub use row_store::StoredRow;

/// Storage facade coordinating schema persistence, row-file IO,
/// and filesystem layout.
///
/// This type keeps the public API stable for the rest of the engine while
/// delegating implementation details to focused subcomponents:
/// - `layout` handles filesystem paths
/// - `schema_store` manages `schema.json`
/// - `row_store` manages table JSONL row files
///
/// Higher-level row mutation policies such as schema-aware validation,
/// UNIQUE checks, row-id-preserving rewrites, and WAL undo helpers should
/// live above this facade.
pub struct Storage {
    layout: StorageLayout,
    schema_store: SchemaStore,
    row_store: RowStore,
}

impl Storage {
    /// Create a storage handle rooted at the given directory.
    ///
    /// # Arguments
    /// * `root` - Directory where schema and table files are stored.
    ///
    /// # Errors
    /// Returns an error if the root directory cannot be created.
    pub fn new(root: impl AsRef<Path>) -> DbResult<Self> {
        let layout = StorageLayout::new(root);
        std::fs::create_dir_all(layout.root())?;

        let schema_store = SchemaStore::new(layout.clone());
        let row_store = RowStore::new(layout.clone());

        Ok(Self {
            layout,
            schema_store,
            row_store,
        })
    }

    /// Load the schema metadata from disk.
    ///
    /// If the schema file does not exist yet, an empty schema is returned.
    pub fn load_schema(&self) -> DbResult<DatabaseSchema> {
        self.schema_store.load_schema()
    }

    /// Persist schema metadata to disk.
    pub fn save_schema(&self, schema: &DatabaseSchema) -> DbResult<()> {
        self.schema_store.save_schema(schema)
    }

    /// Create a new table by creating the data file first and then updating schema metadata.
    ///
    /// This ordering avoids leaving schema metadata pointing to a table file that failed
    /// to be created.
    pub fn create_table(&self, table: TableSchema) -> DbResult<()> {
        let table_path = self.layout.table_path(&table.name);
        if table_path.exists() {
            return Err(DbError::TableExists(table.name));
        }

        std::fs::File::create(&table_path)?;

        if let Err(error) = self.schema_store.create_table_schema(table.clone()) {
            let _ = std::fs::remove_file(&table_path);
            return Err(error);
        }

        Ok(())
    }

    /// Drop an existing table by removing the data file first and then deleting schema metadata.
    ///
    /// This ordering avoids metadata-only drops when file deletion fails.
    pub fn drop_table(&self, table: &str, if_exists: bool) -> DbResult<()> {
        let table_path = self.layout.table_path(table);
        if table_path.exists() {
            std::fs::remove_file(&table_path)?;
        } else if !if_exists {
            return Err(DbError::TableNotFound(table.to_string()));
        }

        self.schema_store.drop_table_schema(table, if_exists)
    }

    /// Rename an existing table in both schema metadata and table data file.
    ///
    /// The table file is renamed first so schema metadata is only updated after filesystem
    /// rename succeeds.
    pub fn rename_table(&self, old_name: &str, new_name: &str) -> DbResult<()> {
        let old_path = self.layout.table_path(old_name);
        if !old_path.exists() {
            return Err(DbError::TableNotFound(old_name.to_string()));
        }

        let new_path = self.layout.table_path(new_name);
        if new_path.exists() {
            return Err(DbError::TableExists(new_name.to_string()));
        }

        std::fs::rename(&old_path, &new_path)?;

        if let Err(error) = self.schema_store.rename_table_schema(old_name, new_name) {
            let _ = std::fs::rename(&new_path, &old_path);
            return Err(error);
        }

        Ok(())
    }

    /// Replace one table schema definition while keeping the same table data file.
    pub fn replace_table_schema(&self, table: &str, new_schema: TableSchema) -> DbResult<()> {
        self.schema_store.replace_table_schema(table, new_schema)
    }

    /// Append one already-prepared stored row to a table file.
    ///
    /// This is a low-level persistence primitive. Higher layers are responsible
    /// for schema-aware validation, UNIQUE checks, and row-id allocation.
    pub fn append_stored_row(&self, table: &str, row: &StoredRow) -> DbResult<()> {
        self.ensure_table_exists(table)?;
        self.row_store.append_stored_row(table, row)
    }

    /// Return the next available internal `row_id` for a table.
    ///
    /// This is a low-level helper used by higher-level mutation coordination.
    pub fn next_row_id(&self, table: &str) -> DbResult<u64> {
        self.ensure_table_exists(table)?;
        self.row_store.next_row_id(table)
    }

    /// Rewrite a table file with the provided stored rows.
    pub fn rewrite_rows(&self, table: &str, rows: &[StoredRow]) -> DbResult<()> {
        let schema = self.load_schema()?;
        let table_schema = schema.get_table(table)?;

        self.ensure_table_exists(table)?;
        constraints::validate_stored_rows(table_schema, rows)?;
        self.row_store.rewrite_rows(table, rows)
    }

    /// Load all stored rows from a table in file order.
    pub fn load_rows(&self, table: &str) -> DbResult<Vec<StoredRow>> {
        self.row_store.load_rows(table)
    }

    /// Scan all stored rows from a table and apply a callback to each row.
    pub fn scan_apply_rows<F>(&self, table: &str, func: F) -> DbResult<()>
    where
        F: FnMut(&StoredRow) -> DbResult<()>,
    {
        self.row_store.scan_rows(table, func)
    }

    /// Scan all user-visible row values from a table and apply a callback to each row.
    pub fn scan_apply<F>(&self, table: &str, mut func: F) -> DbResult<()>
    where
        F: FnMut(&[Value]) -> DbResult<()>,
    {
        self.scan_apply_rows(table, |row| func(&row.values))
    }

    /// Return one stored row by internal `row_id`.
    pub fn find_row_by_id(&self, table: &str, row_id: u64) -> DbResult<Option<StoredRow>> {
        self.row_store.find_row_by_id(table, row_id)
    }

    /// Return the path of the WAL file.
    pub(crate) fn wal_path(&self) -> std::path::PathBuf {
        self.layout.wal_path()
    }

    /// Return the path of a table data file.
    pub(crate) fn table_path(&self, table: &str) -> std::path::PathBuf {
        self.layout.table_path(table)
    }

    pub(crate) fn ensure_table_exists(&self, table: &str) -> DbResult<()> {
        let table_path = self.layout.table_path(table);
        if !table_path.exists() {
            return Err(DbError::TableNotFound(table.to_string()));
        }
        Ok(())
    }
}
