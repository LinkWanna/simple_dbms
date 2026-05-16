use std::fs;

use crate::error::DbResult;
use crate::storage::StoredRow;
use super::super::Engine;

impl Engine {
    // ── Table-level WAL undo helpers ───────────────────────────────────

    /// Remove a table data file if it exists.
    pub(super) fn drop_table_file_if_exists(&self, table: &str) -> DbResult<()> {
        let table_path = self.storage.table_path(table);
        if table_path.exists() {
            fs::remove_file(table_path)?;
        }
        Ok(())
    }

    /// Restore a table data file directly from WAL payload without requiring the
    /// current schema to already contain the table.
    pub(super) fn restore_table_file(&self, table: &str, rows: &[StoredRow]) -> DbResult<()> {
        // Use force_rewrite_rows because this is called during WAL recovery
        // when the table may not exist on disk yet.
        self.storage.force_rewrite_rows(table, rows)
    }
}
