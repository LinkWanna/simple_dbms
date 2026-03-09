mod alter_table;
mod create_table;
mod delete;
mod drop_table;
mod insert;
mod select;
mod transaction;
mod update;
mod where_comparisons;

use std::path::Path;

use crate::engine::{Engine, ExecutionResult};
use crate::error::DbError;
use crate::schema::Value;
use tempfile::TempDir;

/// Shared test context that keeps the temporary engine storage alive.
pub(super) struct TestContext {
    pub(super) _dir: TempDir,
    pub(super) engine: Engine,
}

/// Create an empty engine test context backed by a fresh temporary directory.
pub(super) fn empty_context() -> TestContext {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path()).unwrap();
    TestContext { _dir: dir, engine }
}

/// Create a new engine instance rooted at the provided path.
///
/// This is useful for restart / WAL recovery tests.
pub(super) fn engine_at(root: impl AsRef<Path>) -> Engine {
    Engine::new(root).unwrap()
}

/// Execute one SQL statement and assert the result is a message with the expected text.
pub(super) fn assert_message(engine: &mut Engine, sql: &str, expected: &str) {
    let result = engine.execute(sql).unwrap();
    match result {
        ExecutionResult::Message(message) => assert_eq!(message, expected),
        other => panic!("expected message result, got {other:?}"),
    }
}

/// Execute one SQL statement and return the `(columns, rows)` payload.
///
/// Panics if the result is not `ExecutionResult::Rows`.
pub(super) fn query_rows(engine: &mut Engine, sql: &str) -> (Vec<String>, Vec<Vec<Value>>) {
    let result = engine.execute(sql).unwrap();
    match result {
        ExecutionResult::Rows { columns, rows } => (columns, rows),
        other => panic!("expected rows result, got {other:?}"),
    }
}

/// Execute one SQL statement and assert the full row result set.
pub(super) fn assert_rows(
    engine: &mut Engine,
    sql: &str,
    expected_columns: &[&str],
    expected_rows: Vec<Vec<Value>>,
) {
    let (columns, rows) = query_rows(engine, sql);
    let expected_columns = expected_columns
        .iter()
        .map(|column| (*column).to_string())
        .collect::<Vec<_>>();

    assert_eq!(columns, expected_columns);
    assert_eq!(rows, expected_rows);
}

/// Execute one SQL statement and assert that it fails with `TableNotFound`.
pub(super) fn assert_table_not_found(engine: &mut Engine, sql: &str, expected_table: &str) {
    let err = engine.execute(sql).unwrap_err();
    match err {
        DbError::TableNotFound(table) => assert_eq!(table, expected_table),
        other => panic!("unexpected error: {other:?}"),
    }
}

/// Execute one SQL statement and assert that it fails with `ColumnNotFound`.
pub(super) fn assert_column_not_found(engine: &mut Engine, sql: &str, expected_column: &str) {
    let err = engine.execute(sql).unwrap_err();
    match err {
        DbError::ColumnNotFound(column) => assert_eq!(column, expected_column),
        other => panic!("unexpected error: {other:?}"),
    }
}

/// Execute one SQL statement and assert that it fails with `Syntax`.
pub(super) fn assert_syntax_error(engine: &mut Engine, sql: &str, expected_message: &str) {
    let err = engine.execute(sql).unwrap_err();
    match err {
        DbError::Syntax(message) => assert_eq!(message, expected_message),
        other => panic!("unexpected error: {other:?}"),
    }
}

/// Execute one SQL statement and assert that it fails with `UniqueConstraintViolation`.
pub(super) fn assert_unique_constraint_error(
    engine: &mut Engine,
    sql: &str,
    expected_column: &str,
) {
    let err = engine.execute(sql).unwrap_err();
    match err {
        DbError::UniqueConstraintViolation { column } => assert_eq!(column, expected_column),
        other => panic!("unexpected error: {other:?}"),
    }
}

/// Execute one SQL statement and assert that it fails with `TypeMismatch`.
pub(super) fn assert_type_mismatch(engine: &mut Engine, sql: &str, expected_column: &str) {
    let err = engine.execute(sql).unwrap_err();
    match err {
        DbError::TypeMismatch { column, .. } => assert_eq!(column, expected_column),
        other => panic!("unexpected error: {other:?}"),
    }
}

/// Execute one SQL statement and assert that it fails with `InvalidValue`.
pub(super) fn assert_invalid_value(
    engine: &mut Engine,
    sql: &str,
    expected_column: &str,
    reason_predicate: impl FnOnce(&str) -> bool,
) {
    let err = engine.execute(sql).unwrap_err();
    match err {
        DbError::InvalidValue { column, reason } => {
            assert_eq!(column, expected_column);
            assert!(
                reason_predicate(&reason),
                "unexpected invalid value reason: {reason}"
            );
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

/// Seed a standard `users` table with `id`, `name`, and `score` columns.
pub(super) fn seed_users() -> TestContext {
    let mut context = empty_context();
    context
        .engine
        .execute("CREATE TABLE users (id INT, name VARCHAR(10), score FLOAT)")
        .unwrap();
    context
        .engine
        .execute("INSERT INTO users VALUES (1, 'alice', 3.5), (2, 'bob', 4.0), (3, 'carol', 4.5), (4, 'dave', 2.5)")
        .unwrap();
    context
}

/// Seed a standard `users` table whose `name` column contains nullable values.
pub(super) fn seed_users_with_nullable_name() -> TestContext {
    let mut context = empty_context();
    context
        .engine
        .execute("CREATE TABLE users (id INT, name VARCHAR(10), score FLOAT)")
        .unwrap();
    context
        .engine
        .execute("INSERT INTO users VALUES (1, 'alice', 3.5), (2, NULL, 4.0), (3, 'carol', 4.5), (4, NULL, 2.5)")
        .unwrap();
    context
}

/// Assert that a query returns a single `id` column with the provided integer ids.
pub(super) fn assert_id_rows(result: ExecutionResult, expected_ids: &[i64]) {
    match result {
        ExecutionResult::Rows { columns, rows } => {
            assert_eq!(columns, vec!["id"]);
            let expected_rows = expected_ids
                .iter()
                .map(|id| vec![Value::Int(*id)])
                .collect::<Vec<_>>();
            assert_eq!(rows, expected_rows);
        }
        other => panic!("expected rows result, got {other:?}"),
    }
}

/// Return the default WAL file path for a given engine root.
pub(super) fn wal_path(root: &Path) -> std::path::PathBuf {
    root.join("wal.log")
}
