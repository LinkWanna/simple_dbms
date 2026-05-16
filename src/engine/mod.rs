use std::path::Path;

use pesqlite::{Stmt, parse_stmt};

use crate::error::{DbError, DbResult};
use crate::schema::Value;
use crate::storage::{Storage, StoredRow};
use crate::wal::Wal;

mod alter_table;
mod constraints;
mod create_table;
mod delete;
mod drop_table;
mod insert;
mod select;
mod transaction;
mod update;
mod where_clause;

use std::fs;

use crate::wal::WalRecord;

/// Result of executing a SQL command.
#[derive(Debug, PartialEq)]
pub enum ExecutionResult {
    /// Statements that do not return rows.
    Message(String),
    /// Result set for SELECT.
    Rows {
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
    },
}

/// Transaction state for a single in-progress transaction.
#[derive(Debug)]
pub(super) struct TransactionState {
    pub(super) wal: Wal,
}

/// Core execution engine that coordinates parsing, schema validation,
/// and storage operations.
pub struct Engine {
    pub(super) storage: Storage,
    pub(super) transaction_state: Option<TransactionState>,
}

impl Engine {
    // ── Construction & startup ──────────────────────────────────────

    /// Create a new engine with the given storage root directory.
    pub fn new(root: impl AsRef<Path>) -> DbResult<Self> {
        let storage = Storage::new(root)?;
        let mut engine = Self {
            storage,
            transaction_state: None,
        };
        engine.recover_from_wal_if_needed()?;
        Ok(engine)
    }

    /// Recover from an unfinished transaction if a WAL file is present.
    fn recover_from_wal_if_needed(&mut self) -> DbResult<()> {
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

    // ── SQL dispatch ────────────────────────────────────────────────

    /// Execute a SQL statement string and return an execution result.
    pub fn execute(&mut self, sql: &str) -> DbResult<ExecutionResult> {
        if sql.is_empty() {
            return Err(DbError::syntax("empty SQL statement"));
        }

        let stmt = parse_stmt(sql).map_err(|e| DbError::Syntax(e.to_string()))?;

        match stmt {
            Stmt::Insert(insert) => self.execute_insert(insert),
            Stmt::Select(select) => self.execute_select(select),
            Stmt::Update(update) => self.execute_update(update),
            Stmt::Delete(delete) => self.execute_delete(delete),
            Stmt::CreateTable(create_table) => self.execute_create_table(create_table),
            Stmt::CreateIndex(_) => Err(DbError::syntax("CREATE INDEX is not supported yet")),
            Stmt::CreateView(_) => Err(DbError::syntax("CREATE VIEW is not supported yet")),
            Stmt::CreateTrigger(_) => Err(DbError::syntax("CREATE TRIGGER is not supported yet")),
            Stmt::AlterTable(alter_table) => self.execute_alter_table(alter_table),
            Stmt::DropTable(drop_table) => self.execute_drop_table(drop_table),
            Stmt::DropIndex(_) => Err(DbError::syntax("DROP INDEX is not supported yet")),
            Stmt::DropView(_) => Err(DbError::syntax("DROP VIEW is not supported yet")),
            Stmt::DropTrigger(_) => Err(DbError::syntax("DROP TRIGGER is not supported yet")),
            Stmt::Begin(begin) => self.execute_begin(begin),
            Stmt::Commit(commit) => self.execute_commit(commit),
            Stmt::Rollback(rollback) => self.execute_rollback(rollback),
            Stmt::Savepoint(_) => Err(DbError::syntax("SAVEPOINT is not supported yet")),
            Stmt::Release(_) => Err(DbError::syntax("RELEASE SAVEPOINT is not supported yet")),
        }
    }

    // ── Row-level helpers (shared internally across WAL recovery) ───

    /// Delete one row identified by internal `row_id`.
    pub(super) fn delete_stored_row_by_id(&self, table: &str, row_id: u64) -> DbResult<()> {
        let mut rows = self.storage.load_rows(table)?;
        let original_len = rows.len();
        rows.retain(|row| row.row_id != row_id);

        if rows.len() == original_len {
            return Err(DbError::InvalidValue {
                column: "row_id".to_string(),
                reason: format!("row id {row_id} not found in table '{table}'"),
            });
        }

        self.storage.rewrite_rows(table, &rows)
    }

    /// Replace one row's values identified by internal `row_id`.
    pub(super) fn replace_stored_row_values(
        &self,
        table: &str,
        row_id: u64,
        values: &[Value],
    ) -> DbResult<()> {
        let schema = self.storage.load_schema()?;
        let table_schema = schema.get_table(table)?;

        table_schema.validate_row(values)?;

        let mut rows = self.storage.load_rows(table)?;
        let mut found = false;

        for row in &mut rows {
            if row.row_id == row_id {
                row.values = values.to_vec();
                found = true;
                break;
            }
        }

        if !found {
            return Err(DbError::InvalidValue {
                column: "row_id".to_string(),
                reason: format!("row id {row_id} not found in table '{table}'"),
            });
        }

        constraints::validate_stored_rows(table_schema, &rows)?;
        self.storage.rewrite_rows(table, &rows)
    }

    /// Restore one deleted row with its original internal `row_id`.
    pub(super) fn restore_stored_row(
        &self,
        table: &str,
        row_id: u64,
        values: &[Value],
    ) -> DbResult<()> {
        let schema = self.storage.load_schema()?;
        let table_schema = schema.get_table(table)?;

        table_schema.validate_row(values)?;
        self.storage.ensure_table_exists(table)?;

        if self.find_row_by_id(table, row_id)?.is_some() {
            return Err(DbError::InvalidValue {
                column: "row_id".to_string(),
                reason: format!("row id {row_id} already exists in table '{table}'"),
            });
        }

        let existing_rows = self.storage.load_rows(table)?;
        constraints::validate_unique_append(table_schema, &existing_rows, values)?;

        let stored_row = StoredRow {
            row_id,
            values: values.to_vec(),
        };

        self.storage.append_stored_row(table, &stored_row)
    }

    /// Remove a table data file if it exists.
    pub(super) fn drop_table_file_if_exists(&self, table: &str) -> DbResult<()> {
        let table_path = self.storage.table_path(table);
        if table_path.exists() {
            fs::remove_file(table_path)?;
        }
        Ok(())
    }

    /// Restore a table data file directly from WAL payload without requiring the
    /// current schema to already contain the table.
    pub(super) fn restore_table_file(&self, table: &str, rows: &[StoredRow]) -> DbResult<()> {
        // Use force_rewrite_rows because this is called during WAL recovery
        // when the table may not exist on disk yet.
        self.storage.force_rewrite_rows(table, rows)
    }

    // ── WAL undo ────────────────────────────────────────────────────

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

    // ── Small helpers ───────────────────────────────────────────────

    /// Return one stored row by internal `row_id`, or `None`.
    fn find_row_by_id(&self, table: &str, row_id: u64) -> DbResult<Option<StoredRow>> {
        let mut found = None;
        self.storage.scan_apply_rows(table, |row| {
            if row.row_id == row_id {
                found = Some(row.clone());
            }
            Ok(())
        })?;
        Ok(found)
    }
}

#[cfg(test)]
mod tests;
