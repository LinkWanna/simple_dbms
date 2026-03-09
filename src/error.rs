use std::io;

use thiserror::Error;

/// Result alias for all DB operations.
pub type DbResult<T> = Result<T, DbError>;

/// Unified error type for the mini DBMS.
///
/// This error covers storage, schema, parsing, and type validation issues.
#[derive(Debug, Error)]
pub enum DbError {
    /// I/O layer error.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// JSON (de)serialization error.
    #[error("Serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    /// Generic syntax/parse error.
    #[error("Syntax error: {0}")]
    Syntax(String),

    /// Attempted to create an already existing table.
    #[error("Table already exists: {0}")]
    TableExists(String),

    /// Attempted to access a non-existent table.
    #[error("Table not found: {0}")]
    TableNotFound(String),

    /// Missing column in schema or query.
    #[error("Column not found: {0}")]
    ColumnNotFound(String),

    /// Column count mismatch between schema and provided values.
    #[error("Column count mismatch: expected {expected}, found {found}")]
    ColumnCountMismatch { expected: usize, found: usize },

    /// Type mismatch for a given column.
    #[error("Type mismatch on column '{column}': expected {expected}, found {found}")]
    TypeMismatch {
        column: String,
        expected: String,
        found: String,
    },

    /// Invalid value for a given column.
    #[error("Invalid value for column '{column}': {reason}")]
    InvalidValue { column: String, reason: String },

    /// UNIQUE constraint violation for a given column.
    #[error("Unique constraint violation on column '{column}': duplicate value")]
    UniqueConstraintViolation { column: String },
}

impl DbError {
    /// Helper to construct a syntax error.
    pub fn syntax<S: Into<String>>(msg: S) -> Self {
        DbError::Syntax(msg.into())
    }
}
