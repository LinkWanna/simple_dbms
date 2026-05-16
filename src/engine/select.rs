use std::collections::HashSet;

use pesqlite::Expr;
use pesqlite::{FromClause, ResultColumn, Select, SelectCore};
#[cfg(feature = "btree")]
use pesqlite::BinaryOp;

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
    /// When a `WHERE` clause is a simple equality on an indexed column,
    /// the index is used to narrow candidate rows before applying the
    /// full filter.
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

        // ── Try index-accelerated lookup ──────────────────────────
        #[cfg(feature = "btree")]
        let candidate_ids =
            Self::try_index_lookup(&self.storage, &schema, &table_name, filter)?;
        #[cfg(not(feature = "btree"))]
        let candidate_ids: Option<HashSet<u64>> = None;

        let mut rows: Vec<Vec<Value>> = Vec::new();

        if let Some(ref ids) = candidate_ids {
            // Index narrowed candidates: read only matching rows.
            let ids_vec: Vec<u64> = ids.iter().copied().collect();
            self.storage
                .read_rows_by_id(&table_name, &ids_vec, |stored_row| {
                    if Self::matches_filter(&table_schema, &stored_row.values, filter)? {
                        let projected: Vec<Value> = projection_indices
                            .iter()
                            .map(|&idx| stored_row.values[idx].clone())
                            .collect();
                        rows.push(projected);
                    }
                    Ok(())
                })?;
        } else {
            // No index match: full scan.
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
        }

        Ok(ExecutionResult::Rows {
            columns: projection_columns,
            rows,
        })
    }

    /// Try to use an index for the WHERE clause.
    ///
    /// Returns `Some(HashSet)` of candidate row_ids when an index matches
    /// a simple equality on the indexed column, or `None` to fall back to
    /// a full scan.
    #[cfg(feature = "btree")]
    fn try_index_lookup(
        storage: &crate::storage::Storage,
        schema: &crate::schema::DatabaseSchema,
        table_name: &str,
        filter: Option<&Expr>,
    ) -> DbResult<Option<HashSet<u64>>> {
        // Look for a simple pattern: WHERE column = literal
        let (col_name, literal_val) = match filter {
            Some(Expr::Binary(lhs, BinaryOp::Eq, rhs)) => {
                let (col, val_expr) = match (lhs.as_ref(), rhs.as_ref()) {
                    (Expr::QualifiedColumn(_, _, col), val) => (col.clone(), val.clone()),
                    (val, Expr::QualifiedColumn(_, _, col)) => (col.clone(), val.clone()),
                    _ => return Ok(None),
                };
                let lit = Self::expr_to_value(val_expr)?;
                (col, lit)
            }
            _ => return Ok(None),
        };

        // Find an index on this column for this table.
        let index = schema
            .indexes
            .values()
            .find(|idx| idx.table_name == table_name && idx.column == col_name);

        let Some(index) = index else {
            return Ok(None);
        };

        // Use the index: hash the literal value and do a range scan.
        let key = super::value_to_key(&literal_val);
        let row_ids = storage.index_range_scan(&index.name, key)?;

        if row_ids.is_empty() {
            // Index exists but no matches — return empty set (no rows match).
            return Ok(Some(HashSet::new()));
        }

        Ok(Some(row_ids.into_iter().collect()))
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
