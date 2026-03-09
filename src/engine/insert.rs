use pesqlite::{Insert, InsertValues};

use super::{Engine, ExecutionResult};
use crate::error::{DbError, DbResult};
use crate::wal::WalRecord;

impl Engine {
    /// Execute `INSERT INTO ... VALUES ...`.
    ///
    /// # Arguments
    /// * `insert` - Parsed insert AST node.
    ///
    /// # Errors
    /// Returns an error if the table is missing, values are invalid,
    /// or unsupported insert forms are used.
    pub(super) fn execute_insert(&mut self, insert: Insert) -> DbResult<ExecutionResult> {
        let table_name = insert.schema_table.name;
        let schema = self.storage.load_schema()?;
        let table_schema = schema.get_table(&table_name)?.clone();

        let explicit_columns = if insert.cols.is_empty() {
            None
        } else {
            let mut column_indices = Vec::with_capacity(insert.cols.len());
            for column_name in &insert.cols {
                let column_index = table_schema.column_index(column_name)?;
                if column_indices.contains(&column_index) {
                    return Err(DbError::syntax(format!(
                        "duplicate column '{}' in INSERT column list",
                        column_name
                    )));
                }
                column_indices.push(column_index);
            }
            Some(column_indices)
        };

        let inserted = match insert.values {
            InsertValues::Values { values, .. } => {
                let mut count = 0usize;
                for expr_row in values {
                    let input_row = expr_row
                        .into_iter()
                        .map(Self::expr_to_value)
                        .collect::<DbResult<Vec<_>>>()?;

                    let row = match &explicit_columns {
                        Some(column_indices) => {
                            if input_row.len() != column_indices.len() {
                                return Err(DbError::ColumnCountMismatch {
                                    expected: column_indices.len(),
                                    found: input_row.len(),
                                });
                            }

                            let explicit_column_names = column_indices
                                .iter()
                                .map(|column_index| {
                                    table_schema.columns[*column_index].name.clone()
                                })
                                .collect::<Vec<_>>();

                            table_schema
                                .materialize_named_row(&explicit_column_names, &input_row)?
                        }
                        None => table_schema.materialize_row(&input_row)?,
                    };

                    let stored_row = self.append_validated_row(&table_name, &row)?;

                    if let Some(transaction_state) = self.transaction_state.as_ref() {
                        transaction_state.wal.append(&WalRecord::InsertRow {
                            table: table_name.clone(),
                            row_id: stored_row.row_id,
                        })?;
                    }

                    count += 1;
                }
                count
            }
            InsertValues::Select { .. } => {
                return Err(DbError::syntax(
                    "INSERT INTO ... SELECT ... is not supported yet",
                ));
            }
            InsertValues::Default => {
                let row = table_schema.materialize_row(&[])?;
                let stored_row = self.append_validated_row(&table_name, &row)?;

                if let Some(transaction_state) = self.transaction_state.as_ref() {
                    transaction_state.wal.append(&WalRecord::InsertRow {
                        table: table_name.clone(),
                        row_id: stored_row.row_id,
                    })?;
                }

                1
            }
        };

        Ok(ExecutionResult::Message(format!(
            "{inserted} row(s) inserted into '{table_name}'"
        )))
    }
}
