use std::path::Path;

use pesqlite::{Affinity, TypeSize, UnaryOp, parse_stmt};
use pesqlite::{BinaryOp, ColumnConstraintType, ColumnDef, Expr, Literal, ResultColumn, Stmt};

use crate::error::{DbError, DbResult};
use crate::schema::{ColumnSchema, ColumnType, TableSchema, Value};
use crate::storage::{Storage, StoredRow};
use crate::wal::Wal;

mod alter_table;
mod create_table;
mod delete;
mod drop_table;
mod insert;
mod select;
mod transaction;
mod update;

use std::fs::{self, File};
use std::io::Write;

use crate::wal::WalRecord;

/// Result of executing a SQL command.
#[derive(Debug, PartialEq)]
pub enum ExecutionResult {
    /// Statements that do not return rows.
    Message(String),
    /// Result set for SELECT.
    Rows {
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
    },
}

/// Transaction state for a single in-progress transaction.
#[derive(Debug)]
pub(super) struct TransactionState {
    pub(super) wal: Wal,
}

/// Core execution engine that coordinates parsing, schema validation,
/// and storage operations.
///
/// Tables are stored directly under the configured root directory.
/// There is no database selection context.
pub struct Engine {
    pub(super) storage: Storage,
    pub(super) transaction_state: Option<TransactionState>,
}

impl Engine {
    /// Create a new engine with the given storage root directory.
    ///
    /// # Arguments
    /// * `root` - Root directory where schema and table files are stored.
    ///
    /// # Errors
    /// Returns an error if the storage layer cannot be initialized.
    pub fn new(root: impl AsRef<Path>) -> DbResult<Self> {
        let storage = Storage::new(root)?;
        let mut engine = Self {
            storage,
            transaction_state: None,
        };
        engine.recover_from_wal_if_needed()?;
        Ok(engine)
    }

    /// Recover from an unfinished transaction if a WAL file is present.
    ///
    /// This teaching DBMS currently uses undo-style WAL records. If the process
    /// exits before `COMMIT` or `ROLLBACK`, the next engine startup automatically
    /// replays the undo records in reverse order and clears the WAL file.
    ///
    /// # Errors
    /// Returns an error if loading WAL records, applying undo, or clearing the
    /// WAL file fails.
    fn recover_from_wal_if_needed(&mut self) -> DbResult<()> {
        let wal = Wal::new(self.storage.wal_path());
        if !wal.exists() {
            return Ok(());
        }

        let records = wal.load_records()?;
        if records.is_empty() {
            wal.clear()?;
            return Ok(());
        }

        for record in records.into_iter().rev() {
            self.apply_wal_undo(record)?;
        }

        wal.clear()?;
        Ok(())
    }

    /// Execute a SQL statement string and return an execution result.
    ///
    /// Supported commands:
    /// - `CREATE TABLE ...`
    /// - `INSERT INTO ...`
    /// - `SELECT ...`
    /// - `UPDATE ...`
    /// - `DELETE ...`
    ///
    /// # Errors
    /// Returns an error if parsing fails, the statement type is unsupported,
    /// or execution encounters a storage/schema problem.
    pub fn execute(&mut self, sql: &str) -> DbResult<ExecutionResult> {
        if sql.is_empty() {
            return Err(DbError::syntax("empty SQL statement"));
        }

        let stmt = parse_stmt(sql).map_err(|e| DbError::Syntax(e.to_string()))?;

        match stmt {
            Stmt::Insert(insert) => self.execute_insert(insert),
            Stmt::Select(select) => self.execute_select(select),
            Stmt::Update(update) => self.execute_update(update),
            Stmt::Delete(delete) => self.execute_delete(delete),
            Stmt::CreateTable(create_table) => self.execute_create_table(create_table),
            Stmt::CreateIndex(_) => Err(DbError::syntax("CREATE INDEX is not supported yet")),
            Stmt::CreateView(_) => Err(DbError::syntax("CREATE VIEW is not supported yet")),
            Stmt::CreateTrigger(_) => Err(DbError::syntax("CREATE TRIGGER is not supported yet")),
            Stmt::AlterTable(alter_table) => self.execute_alter_table(alter_table),
            Stmt::DropTable(drop_table) => self.execute_drop_table(drop_table),
            Stmt::DropIndex(_) => Err(DbError::syntax("DROP INDEX is not supported yet")),
            Stmt::DropView(_) => Err(DbError::syntax("DROP VIEW is not supported yet")),
            Stmt::DropTrigger(_) => Err(DbError::syntax("DROP TRIGGER is not supported yet")),
            Stmt::Begin(begin) => self.execute_begin(begin),
            Stmt::Commit(commit) => self.execute_commit(commit),
            Stmt::Rollback(rollback) => self.execute_rollback(rollback),
            Stmt::Savepoint(_) => Err(DbError::syntax("SAVEPOINT is not supported yet")),
            Stmt::Release(_) => Err(DbError::syntax("RELEASE SAVEPOINT is not supported yet")),
        }
    }

    /// Append one user-visible row after validating it against the stored schema
    /// and assigning a new internal `row_id`.
    ///
    /// # Arguments
    /// * `table` - Target table name.
    /// * `row` - User-visible row values in schema column order.
    ///
    /// # Errors
    /// Returns an error if the table is missing, the row violates schema or
    /// UNIQUE constraints, or the row cannot be appended.
    pub(super) fn append_validated_row(&self, table: &str, row: &[Value]) -> DbResult<StoredRow> {
        let schema = self.storage.load_schema()?;
        let table_schema = schema.get_table(table)?;

        table_schema.validate_row(row)?;
        self.storage.ensure_table_exists(table)?;

        let existing_rows = self.storage.load_rows(table)?;
        crate::storage::constraints::validate_unique_append(table_schema, &existing_rows, row)?;

        let row_id = self.storage.next_row_id(table)?;
        let stored_row = StoredRow {
            row_id,
            values: row.to_vec(),
        };

        self.storage.append_stored_row(table, &stored_row)?;
        Ok(stored_row)
    }

    /// Rewrite a table file from user-visible row values while preserving
    /// existing row ids by file order and assigning new ids for appended rows.
    ///
    /// # Arguments
    /// * `table` - Target table name.
    /// * `rows` - User-visible rows in final file order.
    ///
    /// # Errors
    /// Returns an error if existing rows cannot be loaded or the rewritten table
    /// contents are invalid.
    pub(super) fn rewrite_table_from_values(
        &self,
        table: &str,
        rows: &[Vec<Value>],
    ) -> DbResult<()> {
        let existing_ids = self
            .storage
            .load_rows(table)?
            .into_iter()
            .map(|row| row.row_id)
            .collect::<Vec<_>>();

        let mut next_row_id = existing_ids.iter().copied().max().unwrap_or(0) + 1;
        let mut stored_rows = Vec::with_capacity(rows.len());

        for (index, row) in rows.iter().enumerate() {
            let row_id = if index < existing_ids.len() {
                existing_ids[index]
            } else {
                let row_id = next_row_id;
                next_row_id += 1;
                row_id
            };

            stored_rows.push(StoredRow {
                row_id,
                values: row.clone(),
            });
        }

        self.storage.rewrite_rows(table, &stored_rows)
    }

    /// Delete one row identified by internal `row_id`.
    ///
    /// # Errors
    /// Returns an error if the row id does not exist or rewriting the table
    /// fails.
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
    ///
    /// # Errors
    /// Returns an error if the row is missing, values violate schema or
    /// constraints, or the table rewrite fails.
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

        crate::storage::constraints::validate_stored_rows(table_schema, &rows)?;
        self.storage.rewrite_rows(table, &rows)
    }

    /// Restore one deleted row with its original internal `row_id`.
    ///
    /// # Errors
    /// Returns an error if the row id already exists, values violate schema or
    /// UNIQUE constraints, or the append fails.
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

        if self.storage.find_row_by_id(table, row_id)?.is_some() {
            return Err(DbError::InvalidValue {
                column: "row_id".to_string(),
                reason: format!("row id {row_id} already exists in table '{table}'"),
            });
        }

        let existing_rows = self.storage.load_rows(table)?;
        crate::storage::constraints::validate_unique_append(table_schema, &existing_rows, values)?;

        let stored_row = StoredRow {
            row_id,
            values: values.to_vec(),
        };

        self.storage.append_stored_row(table, &stored_row)
    }

    /// Remove a table data file if it exists.
    ///
    /// # Errors
    /// Returns an error if the file exists but cannot be removed.
    pub(super) fn drop_table_file_if_exists(&self, table: &str) -> DbResult<()> {
        let table_path = self.storage.table_path(table);
        if table_path.exists() {
            fs::remove_file(table_path)?;
        }
        Ok(())
    }

    /// Restore a table data file directly from WAL payload without requiring the
    /// current schema to already contain the table.
    ///
    /// This is needed during rollback because WAL undo is replayed in reverse
    /// order, and the table file may need to be recreated before the schema
    /// metadata record is restored.
    ///
    /// # Arguments
    /// * `table` - Table name whose data file should be recreated.
    /// * `rows` - Full stored-row contents to write back as JSONL.
    ///
    /// # Errors
    /// Returns an error if the table file cannot be created or rewritten.
    pub(super) fn restore_table_file(&self, table: &str, rows: &[StoredRow]) -> DbResult<()> {
        let table_path = self.storage.table_path(table);
        let mut file = File::create(&table_path)?;
        for row in rows {
            let line = serde_json::to_string(row)?;
            file.write_all(line.as_bytes())?;
            file.write_all(b"\n")?;
        }
        file.flush()?;
        Ok(())
    }

    /// Apply one WAL undo record.
    ///
    /// Undo records are replayed in reverse order during rollback.
    ///
    /// # Errors
    /// Returns an error if any restoration step fails.
    pub(super) fn apply_wal_undo(&mut self, record: WalRecord) -> DbResult<()> {
        match record {
            WalRecord::InsertRow { table, row_id } => self.undo_insert_row(&table, row_id),
            WalRecord::UpdateRow {
                table,
                row_id,
                old_values,
            } => self.replace_stored_row_values(&table, row_id, &old_values),
            WalRecord::DeleteRow {
                table,
                row_id,
                old_values,
            } => self.restore_stored_row(&table, row_id, &old_values),
            WalRecord::RewriteTable { table, old_rows } => {
                self.storage.rewrite_rows(&table, &old_rows)
            }
            WalRecord::ReplaceSchema { old_schema } => self.storage.save_schema(&old_schema),
            WalRecord::DropTableFile { table } => self.drop_table_file_if_exists(&table),
            WalRecord::RestoreTableFile { table, rows } => self.restore_table_file(&table, &rows),
            WalRecord::RenameTable { old_name, new_name } => {
                self.storage.rename_table(&new_name, &old_name)
            }
        }
    }

    /// Undo one inserted row by removing the row with the matching internal
    /// `row_id`.
    ///
    /// # Errors
    /// Returns an error if the row cannot be removed.
    fn undo_insert_row(&self, table: &str, row_id: u64) -> DbResult<()> {
        match self.storage.find_row_by_id(table, row_id)? {
            Some(_) => self.delete_stored_row_by_id(table, row_id),
            None => Ok(()),
        }
    }

    /// Build output projection information for a select list.
    ///
    /// # Arguments
    /// * `table_schema` - Source table schema.
    /// * `result_columns` - Parsed select projection list.
    ///
    /// # Errors
    /// Returns an error if unsupported projection forms are used.
    pub(super) fn build_projection(
        table_schema: &TableSchema,
        result_columns: &[ResultColumn],
    ) -> DbResult<(Vec<usize>, Vec<String>)> {
        let mut indices = Vec::new();
        let mut names = Vec::new();

        for result_column in result_columns {
            match result_column {
                ResultColumn::Star => {
                    for (index, column) in table_schema.columns.iter().enumerate() {
                        indices.push(index);
                        names.push(column.name.clone());
                    }
                }
                ResultColumn::Expr(expr, alias) => {
                    let column_name = match expr {
                        Expr::QualifiedColumn(_, _, column_name) => column_name,
                        _ => {
                            return Err(DbError::syntax(
                                "only direct column projection is supported",
                            ));
                        }
                    };

                    let index = table_schema.column_index(column_name)?;
                    indices.push(index);
                    names.push(alias.clone().unwrap_or_else(|| column_name.clone()));
                }
            }
        }

        Ok((indices, names))
    }

    /// Evaluate whether a row matches a supported `WHERE` filter.
    ///
    /// Evaluate whether a row matches a supported `WHERE` filter.
    ///
    /// Currently supports:
    /// - no filter
    /// - comparison predicates such as `column = literal`
    /// - recursive boolean expression trees using `AND`, `OR`, and `NOT`
    ///
    /// # Arguments
    /// * `table_schema` - Source table schema.
    /// * `row` - Candidate row.
    /// * `filter` - Optional where expression.
    ///
    /// # Errors
    /// Returns an error if the filter uses unsupported syntax.
    pub(super) fn matches_filter(
        table_schema: &TableSchema,
        row: &[Value],
        filter: Option<&Expr>,
    ) -> DbResult<bool> {
        let Some(expr) = filter else {
            return Ok(true);
        };

        Self::evaluate_where_expr(table_schema, row, expr)
    }

    /// Recursively evaluate a parsed boolean `WHERE` expression tree.
    ///
    /// # Arguments
    /// * `table_schema` - Source table schema.
    /// * `row` - Candidate row.
    /// * `expr` - Expression node to evaluate.
    ///
    /// # Errors
    /// Returns an error if the expression contains unsupported nodes.
    fn evaluate_where_expr(
        table_schema: &TableSchema,
        row: &[Value],
        expr: &Expr,
    ) -> DbResult<bool> {
        match expr {
            Expr::Binary(lhs, op, rhs) => match op {
                BinaryOp::LogicalAnd => {
                    Ok(Self::evaluate_where_expr(table_schema, row, lhs.as_ref())?
                        && Self::evaluate_where_expr(table_schema, row, rhs.as_ref())?)
                }
                BinaryOp::LogicalOr => {
                    Ok(Self::evaluate_where_expr(table_schema, row, lhs.as_ref())?
                        || Self::evaluate_where_expr(table_schema, row, rhs.as_ref())?)
                }
                BinaryOp::Is | BinaryOp::IsNot => {
                    let left = Self::resolve_operand_value(table_schema, row, lhs.as_ref())?;
                    let right = Self::resolve_operand_value(table_schema, row, rhs.as_ref())?;
                    match right {
                        Value::Null => Ok(if matches!(op, BinaryOp::Is) {
                            matches!(left, Value::Null)
                        } else {
                            !matches!(left, Value::Null)
                        }),
                        _ => Err(DbError::syntax(
                            "WHERE currently only supports IS NULL and IS NOT NULL for IS operators",
                        )),
                    }
                }
                _ => {
                    let left = Self::resolve_operand_value(table_schema, row, lhs.as_ref())?;
                    let right = Self::resolve_operand_value(table_schema, row, rhs.as_ref())?;
                    Self::compare_values(&left, op, &right)
                }
            },
            Expr::Unary(UnaryOp::LogicalNot, inner) => Ok(!Self::evaluate_where_expr(
                table_schema,
                row,
                inner.as_ref(),
            )?),
            Expr::NullJudge(inner, is_null) => {
                let value = Self::resolve_operand_value(table_schema, row, inner.as_ref())?;
                let value_is_null = matches!(value, Value::Null);
                Ok(if *is_null {
                    value_is_null
                } else {
                    !value_is_null
                })
            }
            Expr::ExprList(exprs) if exprs.len() == 1 => {
                Self::evaluate_where_expr(table_schema, row, &exprs[0])
            }
            _ => Err(DbError::syntax(
                "WHERE currently only supports boolean expression trees composed of comparisons, IS NULL, IS NOT NULL, AND, OR, and NOT",
            )),
        }
    }

    /// Resolve an expression operand into a runtime value.
    ///
    /// # Arguments
    /// * `table_schema` - Source table schema.
    /// * `row` - Candidate row.
    /// * `expr` - Operand expression.
    ///
    /// # Errors
    /// Returns an error if the operand cannot be reduced to a row value or literal.
    fn resolve_operand_value(
        table_schema: &TableSchema,
        row: &[Value],
        expr: &Expr,
    ) -> DbResult<Value> {
        match expr {
            Expr::QualifiedColumn(_, _, column_name) => {
                let column_index = table_schema.column_index(column_name)?;
                Ok(row[column_index].clone())
            }
            _ => Self::expr_to_value(expr.clone()),
        }
    }

    /// Compare two runtime values with a supported SQL binary comparison operator.
    ///
    /// # Arguments
    /// * `left` - Value from the current row.
    /// * `op` - Parsed binary operator.
    /// * `right` - Literal value from the filter.
    ///
    /// # Errors
    /// Returns an error if the operator is unsupported for `WHERE`, or if the
    /// value types are incompatible for comparison.
    pub(super) fn compare_values(left: &Value, op: &BinaryOp, right: &Value) -> DbResult<bool> {
        match op {
            BinaryOp::Eq => Ok(left == right),
            BinaryOp::Ne => Ok(left != right),
            BinaryOp::Gt => Self::ordered_compare(left, right, |ordering| ordering.is_gt()),
            BinaryOp::Ge => {
                Self::ordered_compare(left, right, |ordering| ordering.is_gt() || ordering.is_eq())
            }
            BinaryOp::Lt => Self::ordered_compare(left, right, |ordering| ordering.is_lt()),
            BinaryOp::Le => {
                Self::ordered_compare(left, right, |ordering| ordering.is_lt() || ordering.is_eq())
            }
            _ => Err(DbError::syntax(
                "WHERE comparison operators currently support =, !=, <>, >, >=, <, <=, IS NULL, and IS NOT NULL, while boolean composition supports AND, OR, and NOT",
            )),
        }
    }

    /// Perform an ordered comparison between two compatible runtime values.
    ///
    /// # Arguments
    /// * `left` - Left-hand value.
    /// * `right` - Right-hand value.
    /// * `predicate` - Ordering predicate to evaluate.
    ///
    /// # Errors
    /// Returns an error if the two values are not comparable.
    fn ordered_compare<F>(left: &Value, right: &Value, predicate: F) -> DbResult<bool>
    where
        F: FnOnce(std::cmp::Ordering) -> bool,
    {
        let ordering = match (left, right) {
            (Value::Int(l), Value::Int(r)) => l.cmp(r),
            (Value::Float(l), Value::Float(r)) => l
                .partial_cmp(r)
                .ok_or_else(|| DbError::syntax("cannot compare NaN float values"))?,
            (Value::Int(l), Value::Float(r)) => (*l as f64)
                .partial_cmp(r)
                .ok_or_else(|| DbError::syntax("cannot compare NaN float values"))?,
            (Value::Float(l), Value::Int(r)) => l
                .partial_cmp(&(*r as f64))
                .ok_or_else(|| DbError::syntax("cannot compare NaN float values"))?,
            (Value::Str(l), Value::Str(r)) => l.cmp(r),
            _ => {
                return Err(DbError::syntax(
                    "WHERE comparison requires compatible operand types",
                ));
            }
        };

        Ok(predicate(ordering))
    }

    /// Convert a parsed SQL expression into a persisted runtime value.
    ///
    /// Supported forms:
    /// - integer literal
    /// - float literal
    /// - string literal
    ///
    /// # Errors
    /// Returns an error if the expression is not a supported literal form.
    pub(super) fn expr_to_value(expr: Expr) -> DbResult<Value> {
        match expr {
            Expr::Literal(literal) => Self::literal_to_value(literal),
            Expr::Unary(_, inner) => Self::expr_to_value(*inner),
            _ => Err(DbError::syntax(
                "only literal values are supported in this context",
            )),
        }
    }

    /// Convert a parsed SQL literal into a runtime value.
    ///
    /// # Errors
    /// Returns an error if the literal type is unsupported.
    pub(super) fn literal_to_value(literal: Literal) -> DbResult<Value> {
        match literal {
            Literal::Integer(v) => Ok(Value::Int(v as i64)),
            Literal::Float(v) => Ok(Value::Float(v)),
            Literal::String(v) => Ok(Value::Str(v)),
            Literal::Null => Ok(Value::Null),
            Literal::Bool(_) => Err(DbError::syntax("BOOL is not supported yet")),
            Literal::Blob(_) => Err(DbError::syntax("BLOB is not supported yet")),
            Literal::CurrentTime | Literal::CurrentDate | Literal::CurrentTimestamp => {
                Err(DbError::syntax("time/date literals are not supported yet"))
            }
        }
    }

    /// Convert a `pesqlite` column definition into the local schema type.
    ///
    /// This function recognizes the following column-level constraints:
    /// - `NOT NULL`
    /// - `UNIQUE`
    /// - `DEFAULT <literal>`
    ///
    /// # Arguments
    /// * `column_def` - Parsed column definition.
    ///
    /// # Errors
    /// Returns an error if the type is missing, unsupported, or if the default
    /// expression is not a supported literal for the target column type.
    pub(super) fn column_def_to_schema(column_def: ColumnDef) -> DbResult<ColumnSchema> {
        let column_name = column_def.name.clone();

        let mut not_null = false;
        let mut unique = false;
        let mut default_value: Option<Value> = None;
        for constraint in &column_def.constraints {
            match &constraint.ty {
                ColumnConstraintType::NotNull => not_null = true,
                ColumnConstraintType::Unique => unique = true,
                ColumnConstraintType::PrimaryKey { .. } => {
                    not_null = true;
                    unique = true;
                }
                ColumnConstraintType::Default(expr) => {
                    if default_value.is_some() {
                        return Err(DbError::syntax(format!(
                            "multiple DEFAULT constraints are not allowed for column '{}'",
                            column_name
                        )));
                    }
                    default_value = Some(Self::expr_to_value(expr.clone())?);
                }
                ColumnConstraintType::Check(..) => todo!(),
                ColumnConstraintType::ForeignKey(..) => todo!(),
            }
        }

        let col_type = match column_def.col_type {
            Some(type_name) => match type_name.affinity {
                Affinity::Integer => ColumnType::Int,
                Affinity::Real => ColumnType::Float,
                Affinity::Text => match type_name.size {
                    Some(TypeSize::MaxSize(size)) => {
                        let len = size.parse::<usize>().map_err(|_| {
                            DbError::syntax(format!(
                                "invalid VARCHAR size for column '{}'",
                                column_name
                            ))
                        })?;
                        ColumnType::Varchar(len)
                    }
                    Some(TypeSize::TypeSize(size, _)) => {
                        let len = size.parse::<usize>().map_err(|_| {
                            DbError::syntax(format!(
                                "invalid VARCHAR size for column '{}'",
                                column_name
                            ))
                        })?;
                        ColumnType::Varchar(len)
                    }
                    None => ColumnType::Varchar(255),
                },
                other => {
                    return Err(DbError::syntax(format!(
                        "unsupported column type for '{}': {:?}",
                        column_name, other
                    )));
                }
            },
            None => {
                return Err(DbError::syntax(format!(
                    "column '{}' must declare a type",
                    column_name
                )));
            }
        };

        let mut schema =
            ColumnSchema::with_nullability(column_def.name, col_type.clone(), not_null);
        schema.unique = unique;

        if let Some(default_value) = default_value {
            TableSchema::new(
                "__default_validation__",
                vec![ColumnSchema::with_nullability(
                    column_name.clone(),
                    col_type.clone(),
                    not_null,
                )],
            )
            .validate_row(&[default_value.clone()])?;

            if matches!(default_value, Value::Null) && not_null {
                return Err(DbError::InvalidValue {
                    column: column_name,
                    reason: "NULL is not allowed for NOT NULL column".to_string(),
                });
            }

            schema.default = Some(default_value);
        }

        Ok(schema)
    }
}

#[cfg(test)]
mod tests;
