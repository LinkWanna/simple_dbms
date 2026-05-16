//! WHERE clause evaluation for SELECT / UPDATE / DELETE.
//!
//! These methods are shared across the three DML statements that support
//! filtering. They live in their own module to keep `mod.rs` lean.

use pesqlite::{BinaryOp, Expr, UnaryOp};

use crate::error::{DbError, DbResult};
use crate::schema::{TableSchema, Value};

use super::Engine;

impl Engine {
    /// Evaluate whether a row matches a supported `WHERE` filter.
    ///
    /// Currently supports:
    /// - no filter (returns `true`)
    /// - comparison predicates such as `column = literal`
    /// - recursive boolean expression trees using `AND`, `OR`, and `NOT`
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

    /// Convert a parsed SQL expression into a runtime value.
    ///
    /// Supported forms: integer literal, float literal, string literal.
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
    pub(super) fn literal_to_value(literal: pesqlite::Literal) -> DbResult<Value> {
        use pesqlite::Literal;
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
}
