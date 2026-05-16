mod row;
mod table;

use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::DbResult;
use crate::schema::{DatabaseSchema, Value};
use crate::storage::StoredRow;

/// Undo-style WAL record used by the teaching DBMS.
///
/// This WAL mixes:
/// - row-level undo records for DML (`INSERT` / `UPDATE` / `DELETE`)
/// - table/schema-level undo records for DDL and table-file restoration
///
/// Records are appended as JSON Lines and replayed in reverse order during
/// rollback or startup recovery.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalRecord {
    /// Undo for appending one row to a table.
    ///
    /// Rollback should remove the row with the specified `row_id`.
    InsertRow { table: String, row_id: u64 },

    /// Undo for updating one existing row.
    ///
    /// Rollback should restore `old_values` for the given `row_id`.
    UpdateRow {
        table: String,
        row_id: u64,
        old_values: Vec<Value>,
    },

    /// Undo for deleting one existing row.
    ///
    /// Rollback should restore the deleted row with its original `row_id`.
    DeleteRow {
        table: String,
        row_id: u64,
        old_values: Vec<Value>,
    },

    /// Undo for replacing the full contents of one table file.
    ///
    /// This remains useful for table-shape-changing DDL such as
    /// `ALTER TABLE ADD COLUMN` / `DROP COLUMN`, where row-level undo is less
    /// convenient than restoring the previous physical table contents.
    RewriteTable {
        table: String,
        old_rows: Vec<StoredRow>,
    },

    /// Undo for replacing the full schema metadata.
    ReplaceSchema { old_schema: DatabaseSchema },

    /// Undo for creating a new table file.
    DropTableFile { table: String },

    /// Undo for restoring one deleted table file.
    RestoreTableFile { table: String, rows: Vec<StoredRow> },

    /// Undo for renaming a table data file.
    RenameTable { old_name: String, new_name: String },
}

/// Minimal write-ahead log used for transaction rollback.
///
/// The log file is append-only during a transaction. On rollback,
/// records are read back and applied in reverse order.
#[derive(Debug, Clone)]
pub struct Wal {
    path: PathBuf,
}

impl Wal {
    /// Create a WAL handle for the given log file path.
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
        }
    }

    /// Returns whether the WAL file currently exists.
    pub fn exists(&self) -> bool {
        self.path.exists()
    }

    /// Create or truncate the WAL file for a new transaction.
    pub fn reset(&self) -> DbResult<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        File::create(&self.path)?;
        Ok(())
    }

    /// Append one undo record to the WAL.
    pub fn append(&self, record: &WalRecord) -> DbResult<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        let line = serde_json::to_string(record)?;
        file.write_all(line.as_bytes())?;
        file.write_all(b"\n")?;
        file.flush()?;
        Ok(())
    }

    /// Load all records from the WAL in append order.
    pub fn load_records(&self) -> DbResult<Vec<WalRecord>> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }

        let file = File::open(&self.path)?;
        let reader = BufReader::new(file);
        let mut records = Vec::new();

        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            records.push(serde_json::from_str(&line)?);
        }

        Ok(records)
    }

    /// Remove the WAL file if it exists.
    pub fn clear(&self) -> DbResult<()> {
        if self.path.exists() {
            fs::remove_file(&self.path)?;
        }
        Ok(())
    }
}

// ── Engine WAL recovery + undo dispatch ────────────────────────────

impl super::Engine {
    /// Recover from an unfinished transaction if a WAL file is present.
    pub(super) fn recover_from_wal_if_needed(&mut self) -> DbResult<()> {
        let wal = Wal::new(self.storage.wal_path());
        if !wal.exists() {
            return Ok(());
        }

        let records = wal.load_records()?;
        if records.is_empty() {
            wal.clear()?;
            return Ok(());
        }

        for record in records.into_iter().rev() {
            self.apply_wal_undo(record)?;
        }

        wal.clear()?;
        Ok(())
    }

    /// Apply one WAL undo record.
    pub(super) fn apply_wal_undo(&mut self, record: WalRecord) -> DbResult<()> {
        match record {
            WalRecord::InsertRow { table, row_id } => self.undo_insert_row(&table, row_id),
            WalRecord::UpdateRow {
                table,
                row_id,
                old_values,
            } => self.replace_stored_row_values(&table, row_id, &old_values),
            WalRecord::DeleteRow {
                table,
                row_id,
                old_values,
            } => self.restore_stored_row(&table, row_id, &old_values),
            WalRecord::RewriteTable { table, old_rows } => {
                self.storage.rewrite_rows(&table, &old_rows)
            }
            WalRecord::ReplaceSchema { old_schema } => self.storage.save_schema(&old_schema),
            WalRecord::DropTableFile { table } => self.drop_table_file_if_exists(&table),
            WalRecord::RestoreTableFile { table, rows } => self.restore_table_file(&table, &rows),
            WalRecord::RenameTable { old_name, new_name } => {
                self.rename_table(&new_name, &old_name)
            }
        }
    }

    /// Undo one inserted row by removing the row with the matching internal
    /// `row_id`.
    fn undo_insert_row(&self, table: &str, row_id: u64) -> DbResult<()> {
        match self.find_row_by_id(table, row_id)? {
            Some(_) => self.delete_stored_row_by_id(table, row_id),
            None => Ok(()),
        }
    }
}
