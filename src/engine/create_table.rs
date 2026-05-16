use pesqlite::{Affinity, ColumnConstraintType, ColumnDef, CreateTableBody};

use crate::error::{DbError, DbResult};
use crate::schema::{ColumnSchema, ColumnType, TableSchema, Value};
use crate::wal::WalRecord;

use super::{Engine, ExecutionResult};

impl Engine {
    // ── Low-level compound operation ──────────────────────────────────

    /// Create a new table: empty data file + schema entry, atomically.
    pub(super) fn create_table(&self, table: TableSchema) -> DbResult<()> {
        let table_name = table.name.clone();
        if self.storage.table_file_exists(&table_name) {
            return Err(DbError::TableExists(table_name));
        }

        self.storage.create_table_file(&table_name)?;

        let schema_result = {
            let mut schema = self.storage.load_schema()?;
            if schema.tables.contains_key(&table_name) {
                return Err(DbError::TableExists(table_name));
            }
            schema.add_table(table)?;
            self.storage.save_schema(&schema)
        };

        if let Err(error) = schema_result {
            let _ = self.storage.remove_table_file(&table_name);
            return Err(error);
        }

        Ok(())
    }

    // ── SQL executor ──────────────────────────────────────────────────
    /// Execute `CREATE TABLE`.
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

        self.create_table(schema)?;

        Ok(ExecutionResult::Message(format!(
            "Table '{table_name}' created"
        )))
    }

    /// Convert a `pesqlite` column definition into the local schema type.
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
                    Some(pesqlite::TypeSize::MaxSize(size)) => {
                        let len = size.parse::<usize>().map_err(|_| {
                            DbError::syntax(format!(
                                "invalid VARCHAR size for column '{}'",
                                column_name
                            ))
                        })?;
                        ColumnType::Varchar(len)
                    }
                    Some(pesqlite::TypeSize::TypeSize(size, _)) => {
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
