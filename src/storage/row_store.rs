use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};

use serde::{Deserialize, Serialize};

use crate::error::{DbError, DbResult};
use crate::schema::Value;

use super::layout::StorageLayout;

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

/// Low-level row file store for table JSONL files.
///
/// This module is responsible only for physical row persistence concerns:
/// - reading table files into `StoredRow` values
/// - appending stored rows
/// - rewriting full table files
/// - locating rows by internal `row_id`
///
/// It does not perform schema or unique-constraint validation. Those concerns
/// should remain in higher-level storage or validation modules.
#[derive(Debug, Clone)]
pub struct RowStore {
    layout: StorageLayout,
}

impl RowStore {
    /// Create a row store rooted at the given directory.
    pub fn new(layout: StorageLayout) -> Self {
        Self { layout }
    }

    /// Ensure a table data file exists.
    ///
    /// # Errors
    /// Returns an error if the table file is missing.
    pub fn ensure_table_exists(&self, table: &str) -> DbResult<()> {
        let table_path = self.table_path(table);
        if !table_path.exists() {
            return Err(DbError::TableNotFound(table.to_string()));
        }
        Ok(())
    }

    /// Load all stored rows from a table in file order.
    ///
    /// # Errors
    /// Returns an error if the table file does not exist or if any row cannot be
    /// decoded from JSONL.
    pub fn load_rows(&self, table: &str) -> DbResult<Vec<StoredRow>> {
        let mut rows = Vec::new();
        self.scan_rows(table, |row| {
            rows.push(row.clone());
            Ok(())
        })?;
        Ok(rows)
    }

    /// Scan all stored rows from a table and apply a callback to each row.
    ///
    /// # Errors
    /// Returns an error if the table file does not exist, row decoding fails,
    /// or the callback returns an error.
    pub fn scan_rows<F>(&self, table: &str, mut func: F) -> DbResult<()>
    where
        F: FnMut(&StoredRow) -> DbResult<()>,
    {
        self.ensure_table_exists(table)?;

        let file = File::open(self.table_path(table))?;
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

    /// Append one stored row to a table file.
    ///
    /// # Errors
    /// Returns an error if the table file does not exist or if the row cannot be
    /// serialized and written.
    pub fn append_stored_row(&self, table: &str, row: &StoredRow) -> DbResult<()> {
        self.ensure_table_exists(table)?;

        let mut file = OpenOptions::new()
            .create(false)
            .append(true)
            .open(self.table_path(table))?;
        let line = serde_json::to_string(row)?;
        file.write_all(line.as_bytes())?;
        file.write_all(b"\n")?;
        Ok(())
    }

    /// Rewrite a table file with the provided stored rows.
    ///
    /// # Errors
    /// Returns an error if the table file does not exist or if the replacement
    /// file cannot be written atomically.
    pub fn rewrite_rows(&self, table: &str, rows: &[StoredRow]) -> DbResult<()> {
        self.ensure_table_exists(table)?;

        let table_path = self.table_path(table);
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

    /// Return one stored row by internal `row_id`.
    ///
    /// # Errors
    /// Returns an error if scanning the table fails.
    pub fn find_row_by_id(&self, table: &str, row_id: u64) -> DbResult<Option<StoredRow>> {
        let mut found = None;

        self.scan_rows(table, |row| {
            if row.row_id == row_id {
                found = Some(row.clone());
            }
            Ok(())
        })?;

        Ok(found)
    }

    /// Return the next available internal row id for a table.
    ///
    /// # Errors
    /// Returns an error if scanning the table file fails.
    pub fn next_row_id(&self, table: &str) -> DbResult<u64> {
        let mut max_row_id = 0u64;
        self.scan_rows(table, |row| {
            max_row_id = max_row_id.max(row.row_id);
            Ok(())
        })?;
        Ok(max_row_id + 1)
    }

    /// Return the path of a table data file.
    pub fn table_path(&self, table: &str) -> std::path::PathBuf {
        self.layout.table_path(table)
    }
}
