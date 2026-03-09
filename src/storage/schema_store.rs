use std::fs::File;
use std::io::BufReader;

use crate::error::{DbError, DbResult};
use crate::schema::{DatabaseSchema, TableSchema};

use super::layout::StorageLayout;

/// Persistent schema metadata store backed by `schema.json`.
///
/// This module is responsible only for reading and writing the database schema
/// file. Higher-level orchestration, such as coordinating schema changes with
/// table data files, should remain in the storage facade.
#[derive(Debug, Clone)]
pub struct SchemaStore {
    layout: StorageLayout,
}

impl SchemaStore {
    /// Create a schema store using the provided storage layout.
    pub fn new(layout: StorageLayout) -> Self {
        Self { layout }
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

    /// Add a new table definition to schema metadata.
    ///
    /// This updates only `schema.json`. Creating the physical table data file is
    /// the responsibility of a higher-level coordinator.
    pub fn create_table_schema(&self, table: TableSchema) -> DbResult<()> {
        let mut schema = self.load_schema()?;
        if schema.tables.contains_key(&table.name) {
            return Err(DbError::TableExists(table.name.clone()));
        }

        schema.add_table(table)?;
        self.save_schema(&schema)
    }

    /// Remove a table definition from schema metadata.
    ///
    /// This updates only `schema.json`. Removing the physical table data file is
    /// the responsibility of a higher-level coordinator.
    pub fn drop_table_schema(&self, table: &str, if_exists: bool) -> DbResult<()> {
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
    ///
    /// This updates only `schema.json`. Renaming the physical table data file is
    /// the responsibility of a higher-level coordinator.
    pub fn rename_table_schema(&self, old_name: &str, new_name: &str) -> DbResult<()> {
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

    /// Replace one existing table definition in schema metadata.
    pub fn replace_table_schema(&self, table: &str, new_schema: TableSchema) -> DbResult<()> {
        let mut schema = self.load_schema()?;
        if !schema.tables.contains_key(table) {
            return Err(DbError::TableNotFound(table.to_string()));
        }

        schema.tables.insert(table.to_string(), new_schema);
        self.save_schema(&schema)
    }
}
