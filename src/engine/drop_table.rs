use crate::engine::{Engine, ExecutionResult};
use crate::error::DbResult;
use crate::wal::WalRecord;

impl Engine {
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

        self.storage.drop_table(&table_name, drop_table.if_exists)?;

        Ok(ExecutionResult::Message(format!(
            "Table '{table_name}' dropped"
        )))
    }
}
