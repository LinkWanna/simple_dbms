use pesqlite::Delete;

use super::{Engine, ExecutionResult};
use crate::error::{DbError, DbResult};
use crate::wal::WalRecord;

impl Engine {
    /// Execute a single-table `DELETE`.
    ///
    /// Supported subset:
    /// - single target table
    /// - optional `WHERE` boolean expression
    /// - no `RETURNING`
    ///
    /// # Arguments
    /// * `delete` - Parsed delete AST node.
    ///
    /// # Errors
    /// Returns an error if unsupported delete features are used, the table
    /// does not exist, or storage rewrite fails.
    pub(super) fn execute_delete(&mut self, delete: Delete) -> DbResult<ExecutionResult> {
        if !delete.return_clause.is_empty() {
            return Err(DbError::syntax("DELETE RETURNING is not supported yet"));
        }

        let table_name = delete.qualified_table.schema_table.name;
        let schema = self.storage.load_schema()?;
        let table_schema = schema.get_table(&table_name)?.clone();

        let filter = delete.where_clause.as_ref();

        let mut retained_rows = Vec::new();
        let mut deleted_count = 0usize;

        self.storage.scan_apply_rows(&table_name, |stored_row| {
            if Self::matches_filter(&table_schema, &stored_row.values, filter)? {
                if let Some(transaction_state) = self.transaction_state.as_ref() {
                    transaction_state.wal.append(&WalRecord::DeleteRow {
                        table: table_name.clone(),
                        row_id: stored_row.row_id,
                        old_values: stored_row.values.clone(),
                    })?;
                }
                deleted_count += 1;
            } else {
                retained_rows.push(stored_row.clone());
            }
            Ok(())
        })?;

        self.storage.rewrite_rows(&table_name, &retained_rows)?;

        Ok(ExecutionResult::Message(format!(
            "{deleted_count} row(s) deleted from '{table_name}'"
        )))
    }
}
