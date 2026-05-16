use super::super::{Engine, constraints};
use crate::error::{DbError, DbResult};
use crate::schema::Value;
use crate::storage::StoredRow;

impl Engine {
    // ── Row-level WAL undo helpers ────────────────────────────────────

    /// Delete one row identified by internal `row_id`.
    pub(super) fn delete_stored_row_by_id(&self, table: &str, row_id: u64) -> DbResult<()> {
        let mut rows = self.storage.load_rows(table)?;
        let original_len = rows.len();
        rows.retain(|row| row.row_id != row_id);

        if rows.len() == original_len {
            return Err(DbError::InvalidValue {
                column: "row_id".to_string(),
                reason: format!("row id {row_id} not found in table '{table}'"),
            });
        }

        self.storage.rewrite_rows(table, &rows)
    }

    /// Replace one row's values identified by internal `row_id`.
    pub(super) fn replace_stored_row_values(
        &self,
        table: &str,
        row_id: u64,
        values: &[Value],
    ) -> DbResult<()> {
        let schema = self.storage.load_schema()?;
        let table_schema = schema.get_table(table)?;

        table_schema.validate_row(values)?;

        let mut rows = self.storage.load_rows(table)?;
        let mut found = false;

        for row in &mut rows {
            if row.row_id == row_id {
                row.values = values.to_vec();
                found = true;
                break;
            }
        }

        if !found {
            return Err(DbError::InvalidValue {
                column: "row_id".to_string(),
                reason: format!("row id {row_id} not found in table '{table}'"),
            });
        }

        constraints::validate_stored_rows(table_schema, &rows)?;
        self.storage.rewrite_rows(table, &rows)
    }

    /// Restore one deleted row with its original internal `row_id`.
    pub(super) fn restore_stored_row(
        &self,
        table: &str,
        row_id: u64,
        values: &[Value],
    ) -> DbResult<()> {
        let schema = self.storage.load_schema()?;
        let table_schema = schema.get_table(table)?;

        table_schema.validate_row(values)?;
        self.storage.ensure_table_exists(table)?;

        if self.find_row_by_id(table, row_id)?.is_some() {
            return Err(DbError::InvalidValue {
                column: "row_id".to_string(),
                reason: format!("row id {row_id} already exists in table '{table}'"),
            });
        }

        let existing_rows = self.storage.load_rows(table)?;
        constraints::validate_unique_append(table_schema, &existing_rows, values)?;

        let stored_row = StoredRow {
            row_id,
            values: values.to_vec(),
        };

        self.storage.append_stored_row(table, &stored_row)
    }

    /// Return one stored row by internal `row_id`, or `None`.
    pub(super) fn find_row_by_id(&self, table: &str, row_id: u64) -> DbResult<Option<StoredRow>> {
        let mut found = None;
        self.storage.scan_apply_rows(table, |row| {
            if row.row_id == row_id {
                found = Some(row.clone());
            }
            Ok(())
        })?;
        Ok(found)
    }
}
