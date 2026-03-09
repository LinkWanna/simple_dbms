use std::collections::{HashMap, HashSet};

use crate::error::{DbError, DbResult};
use crate::schema::{TableSchema, Value};

use super::StoredRow;

/// Validate a full stored-row replacement set for schema correctness,
/// internal row-id uniqueness, and UNIQUE column constraints.
pub fn validate_stored_rows(table_schema: &TableSchema, rows: &[StoredRow]) -> DbResult<()> {
    validate_row_ids_unique(rows)?;
    validate_rows_against_schema(table_schema, rows)?;
    validate_unique_stored_rows(table_schema, rows)
}

/// Validate that appending one row would not violate any UNIQUE columns.
pub fn validate_unique_append(
    table_schema: &TableSchema,
    existing_rows: &[StoredRow],
    row: &[Value],
) -> DbResult<()> {
    let unique_columns = unique_column_indices(table_schema);
    if unique_columns.is_empty() {
        return Ok(());
    }

    let incoming_keys = row_unique_keys(table_schema, row, &unique_columns)?;

    for existing_row in existing_rows {
        for (column_name, incoming_value) in &incoming_keys {
            if matches!(incoming_value, Value::Null) {
                continue;
            }

            let column_index = table_schema.column_index(column_name)?;
            if existing_row.values[column_index] == *incoming_value {
                return Err(DbError::UniqueConstraintViolation {
                    column: column_name.clone(),
                });
            }
        }
    }

    Ok(())
}

/// Validate that a complete stored-row replacement set does not violate
/// any UNIQUE column constraints.
pub fn validate_unique_stored_rows(table_schema: &TableSchema, rows: &[StoredRow]) -> DbResult<()> {
    let unique_columns = unique_column_indices(table_schema);
    if unique_columns.is_empty() {
        return Ok(());
    }

    let mut seen_by_column: HashMap<String, HashSet<String>> = HashMap::new();

    for row in rows {
        for (column_index, column_name) in &unique_columns {
            if matches!(row.values[*column_index], Value::Null) {
                continue;
            }

            let key = serde_json::to_string(&row.values[*column_index])?;
            let seen = seen_by_column.entry(column_name.clone()).or_default();

            if !seen.insert(key) {
                return Err(DbError::UniqueConstraintViolation {
                    column: column_name.clone(),
                });
            }
        }
    }

    Ok(())
}

/// Return all UNIQUE column indices declared on the table schema.
pub fn unique_column_indices(table_schema: &TableSchema) -> Vec<(usize, String)> {
    table_schema
        .columns
        .iter()
        .enumerate()
        .filter(|(_, column)| column.unique)
        .map(|(index, column)| (index, column.name.clone()))
        .collect()
}

/// Build a map of UNIQUE column names to their incoming values for one row.
pub fn row_unique_keys(
    table_schema: &TableSchema,
    row: &[Value],
    unique_columns: &[(usize, String)],
) -> DbResult<Vec<(String, Value)>> {
    let mut result = Vec::with_capacity(unique_columns.len());
    for (column_index, column_name) in unique_columns {
        let _ = table_schema.column_index(column_name)?;
        result.push((column_name.clone(), row[*column_index].clone()));
    }
    Ok(result)
}

fn validate_rows_against_schema(table_schema: &TableSchema, rows: &[StoredRow]) -> DbResult<()> {
    for row in rows {
        table_schema.validate_row(&row.values)?;
    }
    Ok(())
}

fn validate_row_ids_unique(rows: &[StoredRow]) -> DbResult<()> {
    let mut seen_row_ids = HashSet::new();

    for row in rows {
        if !seen_row_ids.insert(row.row_id) {
            return Err(DbError::syntax(format!(
                "duplicate internal row id {} detected",
                row.row_id
            )));
        }
    }

    Ok(())
}
