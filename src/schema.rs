use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::error::{DbError, DbResult};

/// Runtime value representation for a single cell.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Value {
    Int(i64),
    Float(f64),
    Str(String),
    Null,
}

impl Value {
    /// Returns the type name for diagnostics.
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Int(_) => "INT",
            Value::Float(_) => "FLOAT",
            Value::Str(_) => "VARCHAR",
            Value::Null => "NULL",
        }
    }
}

/// Supported column types in the mini-DBMS.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ColumnType {
    Int,
    Float,
    /// VARCHAR with maximum length (inclusive).
    Varchar(usize),
}

impl ColumnType {
    /// Returns the string name of the column type.
    pub fn type_name(&self) -> String {
        match self {
            ColumnType::Int => "INT".to_string(),
            ColumnType::Float => "FLOAT".to_string(),
            ColumnType::Varchar(len) => format!("VARCHAR({len})"),
        }
    }
}

/// Schema definition for a single column.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ColumnSchema {
    pub name: String,
    pub col_type: ColumnType,
    pub not_null: bool,
    pub unique: bool,
    pub default: Option<Value>,
}

impl ColumnSchema {
    /// Construct a new nullable, non-unique column schema.
    #[allow(dead_code)]
    pub fn new(name: impl Into<String>, col_type: ColumnType) -> Self {
        ColumnSchema {
            name: name.into(),
            col_type,
            not_null: false,
            unique: false,
            default: None,
        }
    }

    /// Construct a new column schema with explicit nullability.
    pub fn with_nullability(name: impl Into<String>, col_type: ColumnType, not_null: bool) -> Self {
        ColumnSchema {
            name: name.into(),
            col_type,
            not_null,
            unique: false,
            default: None,
        }
    }
}

/// Schema definition for a table (name + ordered columns).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnSchema>,
}

impl TableSchema {
    /// Create a new table schema with provided columns.

    pub fn new(name: impl Into<String>, columns: Vec<ColumnSchema>) -> Self {
        TableSchema {
            name: name.into(),
            columns,
        }
    }

    /// Find the index of a column by name (case-insensitive).
    pub fn column_index(&self, column: &str) -> DbResult<usize> {
        self.columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(column))
            .ok_or_else(|| DbError::ColumnNotFound(column.to_string()))
    }

    /// Build a full row by filling omitted trailing columns with declared defaults.
    ///
    /// If a column has no explicit default, `NULL` is used as the implicit default.
    pub fn materialize_row(&self, values: &[Value]) -> DbResult<Vec<Value>> {
        if values.len() > self.columns.len() {
            return Err(DbError::ColumnCountMismatch {
                expected: self.columns.len(),
                found: values.len(),
            });
        }

        let mut row = Vec::with_capacity(self.columns.len());
        for (index, column) in self.columns.iter().enumerate() {
            let value = values
                .get(index)
                .cloned()
                .or_else(|| column.default.clone())
                .unwrap_or(Value::Null);
            row.push(value);
        }

        self.validate_row(&row)?;
        Ok(row)
    }

    /// Build a full row from an explicit column list and corresponding values.
    ///
    /// Unspecified columns are filled with their declared default value, or `NULL`
    /// when no explicit default is defined.
    pub fn materialize_named_row(
        &self,
        columns: &[String],
        values: &[Value],
    ) -> DbResult<Vec<Value>> {
        if columns.len() != values.len() {
            return Err(DbError::ColumnCountMismatch {
                expected: columns.len(),
                found: values.len(),
            });
        }

        let mut row = Vec::with_capacity(self.columns.len());
        for column in &self.columns {
            let value = column.default.clone().unwrap_or(Value::Null);
            row.push(value);
        }

        for (column_name, value) in columns.iter().zip(values.iter()) {
            let column_index = self.column_index(column_name)?;
            row[column_index] = value.clone();
        }

        self.validate_row(&row)?;
        Ok(row)
    }

    /// Validate unnamed row values against schema order.
    pub fn validate_row(&self, values: &[Value]) -> DbResult<()> {
        if values.len() != self.columns.len() {
            return Err(DbError::ColumnCountMismatch {
                expected: self.columns.len(),
                found: values.len(),
            });
        }

        for (col, val) in self.columns.iter().zip(values.iter()) {
            Self::validate_value(col, val)?;
        }

        Ok(())
    }

    fn validate_value(col: &ColumnSchema, val: &Value) -> DbResult<()> {
        if matches!(val, Value::Null) {
            if col.not_null {
                return Err(DbError::InvalidValue {
                    column: col.name.clone(),
                    reason: "NULL is not allowed for NOT NULL column".to_string(),
                });
            }
            return Ok(());
        }

        match (&col.col_type, val) {
            (ColumnType::Int, Value::Int(_)) => Ok(()),
            (ColumnType::Float, Value::Float(_)) => Ok(()),
            (ColumnType::Varchar(max), Value::Str(s)) => {
                if s.len() <= *max {
                    Ok(())
                } else {
                    Err(DbError::InvalidValue {
                        column: col.name.clone(),
                        reason: format!("length {} exceeds maximum {}", s.len(), max),
                    })
                }
            }
            (expected, found) => Err(DbError::TypeMismatch {
                column: col.name.clone(),
                expected: expected.type_name(),
                found: found.type_name().to_string(),
            }),
        }
    }

    /// Returns column names in declaration order.
    #[allow(dead_code)]
    pub fn column_names(&self) -> Vec<String> {
        self.columns.iter().map(|c| c.name.clone()).collect()
    }
}

/// Schema definition for a database (name + table map).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseSchema {
    pub name: String,
    pub tables: HashMap<String, TableSchema>,
}

impl DatabaseSchema {
    /// Create a new empty database schema.
    pub fn new(name: impl Into<String>) -> Self {
        DatabaseSchema {
            name: name.into(),
            tables: HashMap::new(),
        }
    }

    /// Add a table schema; errors if the table already exists.
    pub fn add_table(&mut self, table: TableSchema) -> DbResult<()> {
        let key = table.name.clone();
        if self.tables.contains_key(&key) {
            return Err(DbError::TableExists(key));
        }
        self.tables.insert(key, table);
        Ok(())
    }

    /// Fetch a table schema by name.
    pub fn get_table(&self, table: &str) -> DbResult<&TableSchema> {
        self.tables
            .get(table)
            .ok_or_else(|| DbError::TableNotFound(table.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_row_happy_path() {
        let schema = TableSchema::new(
            "users",
            vec![
                ColumnSchema::new("id", ColumnType::Int),
                ColumnSchema::new("name", ColumnType::Varchar(10)),
                ColumnSchema::new("score", ColumnType::Float),
            ],
        );
        let row = vec![
            Value::Int(1),
            Value::Str("alice".into()),
            Value::Float(3.14),
        ];
        assert!(schema.validate_row(&row).is_ok());
    }

    #[test]
    fn validate_nullable_column_accepts_null() {
        let schema = TableSchema::new(
            "users",
            vec![
                ColumnSchema::new("id", ColumnType::Int),
                ColumnSchema::new("name", ColumnType::Varchar(10)),
            ],
        );
        let row = vec![Value::Int(1), Value::Null];
        assert!(schema.validate_row(&row).is_ok());
    }

    #[test]
    fn validate_not_null_column_rejects_null() {
        let schema = TableSchema::new(
            "users",
            vec![
                ColumnSchema::with_nullability("id", ColumnType::Int, true),
                ColumnSchema::new("name", ColumnType::Varchar(10)),
            ],
        );
        let row = vec![Value::Null, Value::Str("alice".into())];
        let err = schema.validate_row(&row).unwrap_err();
        match err {
            DbError::InvalidValue { column, .. } => assert_eq!(column, "id"),
            _ => panic!("unexpected error: {err:?}"),
        }
    }

    #[test]
    fn validate_row_type_mismatch() {
        let schema = TableSchema::new(
            "users",
            vec![
                ColumnSchema::new("id", ColumnType::Int),
                ColumnSchema::new("name", ColumnType::Varchar(5)),
            ],
        );
        let row = vec![Value::Str("oops".into()), Value::Str("bob".into())];
        let err = schema.validate_row(&row).unwrap_err();
        match err {
            DbError::TypeMismatch { column, .. } => assert_eq!(column, "id"),
            _ => panic!("unexpected error: {err:?}"),
        }
    }

    #[test]
    fn validate_row_length_exceeded() {
        let schema = TableSchema::new(
            "users",
            vec![ColumnSchema::new("name", ColumnType::Varchar(3))],
        );
        let row = vec![Value::Str("toolong".into())];
        let err = schema.validate_row(&row).unwrap_err();
        match err {
            DbError::InvalidValue { column, .. } => assert_eq!(column, "name"),
            _ => panic!("unexpected error: {err:?}"),
        }
    }
}
