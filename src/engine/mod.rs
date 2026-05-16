use std::path::Path;

use pesqlite::{Stmt, parse_stmt};

use crate::error::{DbError, DbResult};
use crate::schema::Value;
use crate::storage::Storage;
use wal::Wal;

mod alter_table;
mod constraints;
mod create_table;
mod delete;
mod drop_table;
mod insert;
mod select;
mod transaction;
mod update;
mod wal;
mod where_clause;

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

}

#[cfg(test)]
mod tests;
