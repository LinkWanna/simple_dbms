use pesqlite::{FromClause, ResultColumn, Select, SelectCore};
use pesqlite::Expr;

use crate::error::{DbError, DbResult};
use crate::schema::{TableSchema, Value};

use super::{Engine, ExecutionResult};

impl Engine {
    /// Execute a single-table `SELECT`.
    ///
    /// Supported subset:
    /// - single table in `FROM`
    /// - `SELECT *` or direct column names
    /// - `WHERE column = literal`
    ///
    /// # Errors
    /// Returns an error if unsupported query features are used.
    pub(super) fn execute_select(&mut self, select: Select) -> DbResult<ExecutionResult> {
        if !select.compound.is_empty() {
            return Err(DbError::syntax("compound SELECT is not supported yet"));
        }
        if !select.order_by.is_empty() {
            return Err(DbError::syntax("ORDER BY is not supported yet"));
        }
        if select.limit.is_some() || select.offset.is_some() {
            return Err(DbError::syntax("LIMIT/OFFSET is not supported yet"));
        }

        let query = match select.core {
            SelectCore::Query(query) => query,
            SelectCore::Values(_) => {
                return Err(DbError::syntax("VALUES query is not supported yet"));
            }
        };

        if query.is_distinct {
            return Err(DbError::syntax("SELECT DISTINCT is not supported yet"));
        }
        if !query.group_by.is_empty() || query.having.is_some() {
            return Err(DbError::syntax("GROUP BY/HAVING is not supported yet"));
        }

        let qualified_table = match query.from_clause {
            Some(FromClause::TableOrQuerys(mut tables)) if tables.len() == 1 => tables.remove(0),
            Some(FromClause::TableOrQuerys(_)) => {
                return Err(DbError::syntax("only single-table SELECT is supported"));
            }
            Some(FromClause::Join(_)) => return Err(DbError::syntax("JOIN is not supported yet")),
            None => return Err(DbError::syntax("SELECT without FROM is not supported")),
        };

        let table_name = qualified_table.schema_table.name;
        let schema = self.storage.load_schema()?;
        let table_schema = schema.get_table(&table_name)?.clone();

        let (projection_indices, projection_columns) =
            Self::build_projection(&table_schema, &query.cols)?;

        let filter = query.where_clause.as_ref();
        let mut rows: Vec<Vec<Value>> = Vec::new();

        self.storage.scan_apply_rows(&table_name, |stored_row| {
            if Self::matches_filter(&table_schema, &stored_row.values, filter)? {
                let projected: Vec<Value> = projection_indices
                    .iter()
                    .map(|&idx| stored_row.values[idx].clone())
                    .collect();
                rows.push(projected);
            }
            Ok(())
        })?;

        Ok(ExecutionResult::Rows {
            columns: projection_columns,
            rows,
        })
    }

    /// Build output projection information for a select list.
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
}
