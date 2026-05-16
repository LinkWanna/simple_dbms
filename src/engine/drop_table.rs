use crate::engine::{Engine, ExecutionResult};
use crate::error::{DbError, DbResult};
use crate::wal::WalRecord;

impl Engine {
    // ── Low-level compound operation ──────────────────────────────────

    /// Drop an existing table: data file + schema entry, atomically.
    pub(super) fn drop_table(&self, table: &str, if_exists: bool) -> DbResult<()> {
        if self.storage.table_file_exists(table) {
            self.storage.remove_table_file(table)?;
        } else if !if_exists {
            return Err(DbError::TableNotFound(table.to_string()));
        }

        let mut schema = self.storage.load_schema()?;
        if schema.tables.remove(table).is_none() {
            if if_exists {
                return Ok(());
            }
            return Err(DbError::TableNotFound(table.to_string()));
        }
        self.storage.save_schema(&schema)
    }

    // ── SQL executor ──────────────────────────────────────────────────
    /// Execute `DROP TABLE`.
    ///
    /// # Arguments
    /// * `drop_table` - Parsed drop-table AST node.
    ///
    /// # Errors
    /// Returns an error if the table does not exist and `IF EXISTS` is not used.
    pub(super) fn execute_drop_table(
        &mut self,
        drop_table: pesqlite::DropTable,
    ) -> DbResult<ExecutionResult> {
        let table_name = drop_table.schema_table.name;

        if let Some(transaction_state) = self.transaction_state.as_ref() {
            let old_schema = self.storage.load_schema()?;
            let old_rows = self.storage.load_rows(&table_name)?;

            transaction_state
                .wal
                .append(&WalRecord::ReplaceSchema { old_schema })?;
            transaction_state.wal.append(&WalRecord::RestoreTableFile {
                table: table_name.clone(),
                rows: old_rows,
            })?;
        }

        self.drop_table(&table_name, drop_table.if_exists)?;

        Ok(ExecutionResult::Message(format!(
            "Table '{table_name}' dropped"
        )))
    }
}
