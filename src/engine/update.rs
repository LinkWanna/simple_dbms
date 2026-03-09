use pesqlite::Update;

use super::{Engine, ExecutionResult};
use crate::error::{DbError, DbResult};
use crate::schema::Value;
use crate::wal::WalRecord;

impl Engine {
    /// Execute a single-table `UPDATE`.
    ///
    /// Supported subset:
    /// - single target table
    /// - `SET column = literal`
    /// - optional `WHERE` boolean expression supported by the engine
    ///
    /// Unsupported features:
    /// - multi-column assignment in one clause
    /// - tuple assignment
    /// - `FROM`
    /// - `RETURNING`
    ///
    /// # Errors
    /// Returns an error if unsupported syntax is used, the table or column
    /// is missing, or the updated row violates schema constraints.
    pub(super) fn execute_update(&mut self, update: Update) -> DbResult<ExecutionResult> {
        if update.from_clause.is_some() {
            return Err(DbError::syntax("UPDATE ... FROM is not supported yet"));
        }

        if !update.return_clause.is_empty() {
            return Err(DbError::syntax("UPDATE ... RETURNING is not supported yet"));
        }

        if update.set_clause.is_empty() {
            return Err(DbError::syntax("UPDATE requires at least one SET clause"));
        }

        let table_name = update.qualified_table.schema_table.name;
        let schema = self.storage.load_schema()?;
        let table_schema = schema.get_table(&table_name)?.clone();

        let assignments = update
            .set_clause
            .into_iter()
            .map(|set_clause| {
                if set_clause.cols.len() != 1 {
                    return Err(DbError::syntax(
                        "only single-column assignment is supported in UPDATE",
                    ));
                }

                let column_name = set_clause.cols[0].clone();
                let column_index = table_schema.column_index(&column_name)?;
                let value = Self::expr_to_value(set_clause.value)?;

                Ok((column_index, value))
            })
            .collect::<DbResult<Vec<(usize, Value)>>>()?;

        let filter = update.where_clause.as_ref();
        let mut rows = self.storage.load_rows(&table_name)?;
        let mut updated_count = 0usize;

        for row in &mut rows {
            if Self::matches_filter(&table_schema, &row.values, filter)? {
                let old_values = row.values.clone();
                let mut next_values = row.values.clone();

                for (column_index, value) in &assignments {
                    next_values[*column_index] = value.clone();
                }

                table_schema.validate_row(&next_values)?;

                if let Some(transaction_state) = self.transaction_state.as_ref() {
                    transaction_state.wal.append(&WalRecord::UpdateRow {
                        table: table_name.clone(),
                        row_id: row.row_id,
                        old_values,
                    })?;
                }

                row.values = next_values;
                updated_count += 1;
            }
        }

        self.storage.rewrite_rows(&table_name, &rows)?;

        Ok(ExecutionResult::Message(format!(
            "{updated_count} row(s) updated in '{table_name}'"
        )))
    }
}
