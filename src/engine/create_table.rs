use pesqlite::CreateTableBody;

use super::{Engine, ExecutionResult};
use crate::error::{DbError, DbResult};
use crate::schema::TableSchema;
use crate::wal::WalRecord;

impl Engine {
    /// Execute `CREATE TABLE`.
    ///
    /// # Arguments
    /// * `create_table` - Parsed create-table AST node.
    ///
    /// # Errors
    /// Returns an error if the table definition uses unsupported syntax
    /// or the table cannot be persisted.
    pub(super) fn execute_create_table(
        &mut self,
        create_table: pesqlite::CreateTable,
    ) -> DbResult<ExecutionResult> {
        let table_name = create_table.schema_table.name;

        let columns = match create_table.body {
            CreateTableBody::Columns { cols, .. } => cols
                .into_iter()
                .map(Self::column_def_to_schema)
                .collect::<DbResult<Vec<_>>>()?,
            CreateTableBody::Select(_) => {
                return Err(DbError::syntax(
                    "CREATE TABLE AS SELECT is not supported yet",
                ));
            }
        };

        let schema = TableSchema::new(table_name.clone(), columns);

        if let Some(transaction_state) = &self.transaction_state {
            let old_schema = self.storage.load_schema()?;
            transaction_state
                .wal
                .append(&WalRecord::ReplaceSchema { old_schema })?;
            transaction_state.wal.append(&WalRecord::DropTableFile {
                table: table_name.clone(),
            })?;
        }

        self.storage.create_table(schema)?;

        Ok(ExecutionResult::Message(format!(
            "Table '{table_name}' created"
        )))
    }
}
