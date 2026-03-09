use crate::error::{DbError, DbResult};
use crate::wal::Wal;

use super::{Engine, ExecutionResult, TransactionState};

impl Engine {
    /// Execute `BEGIN`.
    ///
    /// Supported subset:
    /// - `BEGIN`
    /// - `BEGIN DEFERRED`
    /// - `BEGIN IMMEDIATE`
    /// - `BEGIN EXCLUSIVE`
    ///
    /// The transaction mode is currently recorded for diagnostics only.
    /// Concurrency semantics are intentionally out of scope for this teaching DBMS.
    ///
    /// # Arguments
    /// * `begin` - Parsed begin-transaction AST node.
    ///
    /// # Errors
    /// Returns an error if a transaction is already active or if the WAL file
    /// cannot be initialized.
    pub(super) fn execute_begin(&mut self, begin: pesqlite::Begin) -> DbResult<ExecutionResult> {
        if self.transaction_state.is_some() {
            return Err(DbError::syntax("nested transactions are not supported yet"));
        }

        let wal = Wal::new(self.storage.wal_path());
        wal.reset()?;

        let _transaction_mode = begin.0;
        self.transaction_state = Some(TransactionState { wal });

        Ok(ExecutionResult::Message("Transaction started".to_string()))
    }

    /// Execute `COMMIT`.
    ///
    /// # Arguments
    /// * `_commit` - Parsed commit AST node.
    ///
    /// # Errors
    /// Returns an error if no transaction is active or if the WAL file cannot
    /// be cleaned up.
    pub(super) fn execute_commit(
        &mut self,
        _commit: pesqlite::Commit,
    ) -> DbResult<ExecutionResult> {
        let transaction_state = self
            .transaction_state
            .take()
            .ok_or_else(|| DbError::syntax("no active transaction"))?;

        transaction_state.wal.clear()?;

        Ok(ExecutionResult::Message(
            "Transaction committed".to_string(),
        ))
    }

    /// Execute `ROLLBACK`.
    ///
    /// Supported subset:
    /// - `ROLLBACK`
    ///
    /// Savepoint-based rollback is not supported yet.
    ///
    /// # Arguments
    /// * `rollback` - Parsed rollback AST node.
    ///
    /// # Errors
    /// Returns an error if no transaction is active, if savepoint rollback is
    /// requested, or if replaying WAL undo records fails.
    pub(super) fn execute_rollback(
        &mut self,
        rollback: pesqlite::Rollback,
    ) -> DbResult<ExecutionResult> {
        if rollback.0.is_some() {
            return Err(DbError::syntax(
                "ROLLBACK TO savepoint is not supported yet",
            ));
        }

        let transaction_state = self
            .transaction_state
            .take()
            .ok_or_else(|| DbError::syntax("no active transaction"))?;

        let records = transaction_state.wal.load_records()?;
        for record in records.into_iter().rev() {
            self.apply_wal_undo(record)?;
        }

        transaction_state.wal.clear()?;

        Ok(ExecutionResult::Message(
            "Transaction rolled back".to_string(),
        ))
    }
}
