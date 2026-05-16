use pesqlite::InsertValues;

use crate::error::DbResult;
use crate::schema::Value;
use crate::storage::StoredRow;
use crate::wal::WalRecord;

use super::constraints;
use super::{Engine, ExecutionResult};

impl Engine {
    /// Execute `INSERT INTO ... VALUES ...`.
    pub(super) fn execute_insert(&mut self, insert: pesqlite::Insert) -> DbResult<ExecutionResult> {
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
                    return Err(crate::error::DbError::syntax(format!(
                        "duplicate column '{}' in INSERT column list",
                        column_name
                    )));
                }
                column_indices.push(column_index);
            }
            Some(column_indices)
        };

        let inserted_rows = match insert.values {
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
                                return Err(crate::error::DbError::ColumnCountMismatch {
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
                return Err(crate::error::DbError::syntax(
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
            "{inserted_rows} row(s) inserted into '{table_name}'"
        )))
    }

    /// Append one user-visible row after validating it against the stored schema
    /// and assigning a new internal `row_id`.
    pub(super) fn append_validated_row(&self, table: &str, row: &[Value]) -> DbResult<StoredRow> {
        let schema = self.storage.load_schema()?;
        let table_schema = schema.get_table(table)?;

        table_schema.validate_row(row)?;
        self.storage.ensure_table_exists(table)?;

        let existing_rows = self.storage.load_rows(table)?;
        constraints::validate_unique_append(table_schema, &existing_rows, row)?;

        let row_id = self.next_row_id(table)?;
        let stored_row = StoredRow {
            row_id,
            values: row.to_vec(),
        };

        self.storage.append_stored_row(table, &stored_row)?;
        Ok(stored_row)
    }

    /// Return the next available internal `row_id` for a table.
    fn next_row_id(&self, table: &str) -> DbResult<u64> {
        let mut max_row_id = 0u64;
        self.storage.scan_apply_rows(table, |row| {
            max_row_id = max_row_id.max(row.row_id);
            Ok(())
        })?;
        Ok(max_row_id + 1)
    }
}
