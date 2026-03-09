use crate::engine::{Engine, ExecutionResult};
use crate::error::{DbError, DbResult};
use crate::schema::Value;
use crate::wal::WalRecord;

impl Engine {
    /// Execute `ALTER TABLE`.
    ///
    /// Supported subset:
    /// - `ALTER TABLE <table> RENAME TO <new_table>`
    /// - `ALTER TABLE <table> RENAME COLUMN <old> TO <new>`
    /// - `ALTER TABLE <table> ADD COLUMN <col_def>`
    /// - `ALTER TABLE <table> DROP COLUMN <col>`
    ///
    /// # Arguments
    /// * `alter_table` - Parsed alter-table AST node.
    ///
    /// # Errors
    /// Returns an error if the table/action is invalid or if rewriting persisted
    /// schema/data fails.
    pub(super) fn execute_alter_table(
        &mut self,
        alter_table: pesqlite::AlterTable,
    ) -> DbResult<ExecutionResult> {
        use pesqlite::AlterTableAction;

        let table_name = alter_table.schema_table.name;

        match alter_table.action {
            AlterTableAction::RenameTable(new_table_name) => {
                let schema = self.storage.load_schema()?;
                let old_schema = schema.clone();

                if let Some(transaction_state) = &self.transaction_state {
                    transaction_state.wal.append(&WalRecord::RenameTable {
                        old_name: table_name.clone(),
                        new_name: new_table_name.clone(),
                    })?;
                    transaction_state
                        .wal
                        .append(&WalRecord::ReplaceSchema { old_schema })?;
                }

                self.storage.rename_table(&table_name, &new_table_name)?;
                Ok(ExecutionResult::Message(format!(
                    "Table '{table_name}' renamed to '{new_table_name}'"
                )))
            }
            AlterTableAction::RenameColumn(old_column_name, new_column_name) => {
                let schema = self.storage.load_schema()?;
                let old_schema = schema.clone();
                let mut table_schema = schema.get_table(&table_name)?.clone();

                if table_schema
                    .columns
                    .iter()
                    .any(|column| column.name.eq_ignore_ascii_case(&new_column_name))
                {
                    return Err(DbError::syntax(format!(
                        "column '{}' already exists in table '{}'",
                        new_column_name, table_name
                    )));
                }

                let column_index = table_schema.column_index(&old_column_name)?;
                table_schema.columns[column_index].name = new_column_name.clone();

                if let Some(transaction_state) = &self.transaction_state {
                    transaction_state
                        .wal
                        .append(&WalRecord::ReplaceSchema { old_schema })?;
                }

                self.storage
                    .replace_table_schema(&table_name, table_schema)?;

                Ok(ExecutionResult::Message(format!(
                    "Column '{old_column_name}' renamed to '{new_column_name}' in '{table_name}'"
                )))
            }
            AlterTableAction::AddColumn(column_def) => {
                let schema = self.storage.load_schema()?;
                let old_schema = schema.clone();
                let original_table_schema = schema.get_table(&table_name)?.clone();
                let mut updated_table_schema = original_table_schema.clone();
                let new_column = Self::column_def_to_schema(column_def)?;

                if updated_table_schema
                    .columns
                    .iter()
                    .any(|column| column.name.eq_ignore_ascii_case(&new_column.name))
                {
                    return Err(DbError::syntax(format!(
                        "column '{}' already exists in table '{}'",
                        new_column.name, table_name
                    )));
                }

                if new_column.not_null && new_column.default.is_none() {
                    let mut has_rows = false;
                    self.storage.scan_apply(&table_name, |_| {
                        has_rows = true;
                        Ok(())
                    })?;

                    if has_rows {
                        return Err(DbError::syntax(
                            "cannot add a NOT NULL column without DEFAULT to a non-empty table",
                        ));
                    }
                }

                updated_table_schema.columns.push(new_column.clone());

                let old_rows = self.storage.load_rows(&table_name)?;

                let mut rewritten_rows = Vec::new();
                self.storage.scan_apply(&table_name, |row| {
                    let mut rewritten_row = row.to_vec();
                    rewritten_row.push(new_column.default.clone().unwrap_or(Value::Null));
                    updated_table_schema.validate_row(&rewritten_row)?;
                    rewritten_rows.push(rewritten_row);
                    Ok(())
                })?;

                if let Some(transaction_state) = &self.transaction_state {
                    transaction_state.wal.append(&WalRecord::RewriteTable {
                        table: table_name.clone(),
                        old_rows,
                    })?;
                    transaction_state
                        .wal
                        .append(&WalRecord::ReplaceSchema { old_schema })?;
                }

                self.storage
                    .replace_table_schema(&table_name, updated_table_schema)?;
                self.rewrite_table_from_values(&table_name, &rewritten_rows)?;

                Ok(ExecutionResult::Message(format!(
                    "Column '{}' added to '{}'",
                    new_column.name, table_name
                )))
            }
            AlterTableAction::DropColumn(column_name) => {
                let schema = self.storage.load_schema()?;
                let old_schema = schema.clone();
                let original_table_schema = schema.get_table(&table_name)?.clone();

                if original_table_schema.columns.len() == 1 {
                    return Err(DbError::syntax("cannot drop the last column of a table"));
                }

                let column_index = original_table_schema.column_index(&column_name)?;
                let mut updated_table_schema = original_table_schema.clone();
                updated_table_schema.columns.remove(column_index);

                let old_rows = self.storage.load_rows(&table_name)?;

                let mut rewritten_rows = Vec::new();
                self.storage.scan_apply(&table_name, |row| {
                    let mut rewritten_row = row.to_vec();
                    rewritten_row.remove(column_index);
                    updated_table_schema.validate_row(&rewritten_row)?;
                    rewritten_rows.push(rewritten_row);
                    Ok(())
                })?;

                if let Some(transaction_state) = &self.transaction_state {
                    transaction_state.wal.append(&WalRecord::RewriteTable {
                        table: table_name.clone(),
                        old_rows,
                    })?;
                    transaction_state
                        .wal
                        .append(&WalRecord::ReplaceSchema { old_schema })?;
                }

                self.storage
                    .replace_table_schema(&table_name, updated_table_schema)?;
                self.rewrite_table_from_values(&table_name, &rewritten_rows)?;

                Ok(ExecutionResult::Message(format!(
                    "Column '{column_name}' dropped from '{table_name}'"
                )))
            }
        }
    }
}
