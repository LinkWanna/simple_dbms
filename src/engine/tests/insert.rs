use super::{
    assert_column_not_found, assert_invalid_value, assert_message, assert_rows,
    assert_syntax_error, assert_type_mismatch, empty_context,
};
use crate::error::DbError;
use crate::schema::Value;

#[test]
fn insert_type_mismatch_fails() {
    let mut context = empty_context();

    context
        .engine
        .execute("CREATE TABLE users (id INT, name VARCHAR(10))")
        .unwrap();

    assert_type_mismatch(
        &mut context.engine,
        "INSERT INTO users VALUES ('bad', 'alice')",
        "id",
    );
}

#[test]
fn insert_valid_row_returns_message() {
    let mut context = empty_context();

    context
        .engine
        .execute("CREATE TABLE users (id INT, name VARCHAR(10))")
        .unwrap();

    assert_message(
        &mut context.engine,
        "INSERT INTO users VALUES (1, 'alice')",
        "1 row(s) inserted into 'users'",
    );
}

#[test]
fn insert_null_into_nullable_column_succeeds() {
    let mut context = empty_context();

    context
        .engine
        .execute("CREATE TABLE users (id INT, name VARCHAR(10))")
        .unwrap();

    assert_message(
        &mut context.engine,
        "INSERT INTO users VALUES (1, NULL)",
        "1 row(s) inserted into 'users'",
    );

    assert_rows(
        &mut context.engine,
        "SELECT * FROM users",
        &["id", "name"],
        vec![vec![Value::Int(1), Value::Null]],
    );
}

#[test]
fn insert_null_into_not_null_column_fails() {
    let mut context = empty_context();

    context
        .engine
        .execute("CREATE TABLE users (id INT NOT NULL, name VARCHAR(10) NOT NULL)")
        .unwrap();

    assert_invalid_value(
        &mut context.engine,
        "INSERT INTO users VALUES (1, NULL)",
        "name",
        |reason| reason.contains("NOT NULL"),
    );
}

#[test]
fn insert_default_values_uses_column_defaults() {
    let mut context = empty_context();

    context
        .engine
        .execute(
            "CREATE TABLE users (id INT DEFAULT 100, name VARCHAR(10) DEFAULT 'guest', score FLOAT DEFAULT 3.5)",
        )
        .unwrap();

    assert_message(
        &mut context.engine,
        "INSERT INTO users DEFAULT VALUES",
        "1 row(s) inserted into 'users'",
    );

    assert_rows(
        &mut context.engine,
        "SELECT * FROM users",
        &["id", "name", "score"],
        vec![vec![
            Value::Int(100),
            Value::Str("guest".to_string()),
            Value::Float(3.5),
        ]],
    );
}

#[test]
fn insert_default_values_uses_null_for_columns_without_default() {
    let mut context = empty_context();

    context
        .engine
        .execute("CREATE TABLE users (id INT DEFAULT 1, name VARCHAR(10), score FLOAT DEFAULT 2.5)")
        .unwrap();

    context
        .engine
        .execute("INSERT INTO users DEFAULT VALUES")
        .unwrap();

    assert_rows(
        &mut context.engine,
        "SELECT * FROM users",
        &["id", "name", "score"],
        vec![vec![Value::Int(1), Value::Null, Value::Float(2.5)]],
    );
}

#[test]
fn insert_default_values_respects_not_null_constraints() {
    let mut context = empty_context();

    context
        .engine
        .execute("CREATE TABLE users (id INT NOT NULL DEFAULT 1, name VARCHAR(10) NOT NULL)")
        .unwrap();

    assert_invalid_value(
        &mut context.engine,
        "INSERT INTO users DEFAULT VALUES",
        "name",
        |reason| reason.contains("NOT NULL"),
    );
}

#[test]
fn insert_default_values_rejects_type_mismatched_default() {
    let mut context = empty_context();

    assert_type_mismatch(
        &mut context.engine,
        "CREATE TABLE users (id INT DEFAULT 'bad')",
        "id",
    );
}

#[test]
fn insert_with_explicit_column_list_succeeds() {
    let mut context = empty_context();

    context
        .engine
        .execute("CREATE TABLE users (id INT, name VARCHAR(10), score FLOAT)")
        .unwrap();

    assert_message(
        &mut context.engine,
        "INSERT INTO users (name, id, score) VALUES ('alice', 1, 3.5)",
        "1 row(s) inserted into 'users'",
    );

    assert_rows(
        &mut context.engine,
        "SELECT * FROM users",
        &["id", "name", "score"],
        vec![vec![
            Value::Int(1),
            Value::Str("alice".to_string()),
            Value::Float(3.5),
        ]],
    );
}

#[test]
fn insert_with_explicit_column_list_uses_defaults_for_omitted_columns() {
    let mut context = empty_context();

    context
        .engine
        .execute(
            "CREATE TABLE users (id INT DEFAULT 100, name VARCHAR(10) DEFAULT 'guest', score FLOAT DEFAULT 3.5)",
        )
        .unwrap();

    context
        .engine
        .execute("INSERT INTO users (name) VALUES ('alice')")
        .unwrap();

    assert_rows(
        &mut context.engine,
        "SELECT * FROM users",
        &["id", "name", "score"],
        vec![vec![
            Value::Int(100),
            Value::Str("alice".to_string()),
            Value::Float(3.5),
        ]],
    );
}

#[test]
fn insert_with_explicit_column_list_uses_null_for_omitted_columns_without_default() {
    let mut context = empty_context();

    context
        .engine
        .execute("CREATE TABLE users (id INT, name VARCHAR(10), score FLOAT DEFAULT 2.5)")
        .unwrap();

    context
        .engine
        .execute("INSERT INTO users (name) VALUES ('alice')")
        .unwrap();

    assert_rows(
        &mut context.engine,
        "SELECT * FROM users",
        &["id", "name", "score"],
        vec![vec![
            Value::Null,
            Value::Str("alice".to_string()),
            Value::Float(2.5),
        ]],
    );
}

#[test]
fn insert_with_explicit_column_list_rejects_unknown_column() {
    let mut context = empty_context();

    context
        .engine
        .execute("CREATE TABLE users (id INT, name VARCHAR(10))")
        .unwrap();

    assert_column_not_found(
        &mut context.engine,
        "INSERT INTO users (nickname) VALUES ('alice')",
        "nickname",
    );
}

#[test]
fn insert_with_explicit_column_list_rejects_duplicate_column() {
    let mut context = empty_context();

    context
        .engine
        .execute("CREATE TABLE users (id INT, name VARCHAR(10))")
        .unwrap();

    assert_syntax_error(
        &mut context.engine,
        "INSERT INTO users (id, id) VALUES (1, 2)",
        "duplicate column 'id' in INSERT column list",
    );
}

#[test]
fn insert_with_explicit_column_list_rejects_value_count_mismatch() {
    let mut context = empty_context();

    context
        .engine
        .execute("CREATE TABLE users (id INT, name VARCHAR(10))")
        .unwrap();

    let err = context
        .engine
        .execute("INSERT INTO users (id, name) VALUES (1)")
        .unwrap_err();

    match err {
        DbError::ColumnCountMismatch { expected, found } => {
            assert_eq!(expected, 2);
            assert_eq!(found, 1);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn insert_multi_row_with_explicit_column_list_succeeds() {
    let mut context = empty_context();

    context
        .engine
        .execute("CREATE TABLE users (id INT, name VARCHAR(10), score FLOAT)")
        .unwrap();

    assert_message(
        &mut context.engine,
        "INSERT INTO users (name, id, score) VALUES ('alice', 1, 3.5), ('bob', 2, 4.0)",
        "2 row(s) inserted into 'users'",
    );

    assert_rows(
        &mut context.engine,
        "SELECT * FROM users",
        &["id", "name", "score"],
        vec![
            vec![
                Value::Int(1),
                Value::Str("alice".to_string()),
                Value::Float(3.5),
            ],
            vec![
                Value::Int(2),
                Value::Str("bob".to_string()),
                Value::Float(4.0),
            ],
        ],
    );
}

#[test]
fn insert_multi_row_with_explicit_column_list_uses_defaults_for_omitted_columns() {
    let mut context = empty_context();

    context
        .engine
        .execute(
            "CREATE TABLE users (id INT DEFAULT 100, name VARCHAR(10), score FLOAT DEFAULT 2.5)",
        )
        .unwrap();

    assert_message(
        &mut context.engine,
        "INSERT INTO users (name) VALUES ('alice'), ('bob')",
        "2 row(s) inserted into 'users'",
    );

    assert_rows(
        &mut context.engine,
        "SELECT * FROM users",
        &["id", "name", "score"],
        vec![
            vec![
                Value::Int(100),
                Value::Str("alice".to_string()),
                Value::Float(2.5),
            ],
            vec![
                Value::Int(100),
                Value::Str("bob".to_string()),
                Value::Float(2.5),
            ],
        ],
    );
}
