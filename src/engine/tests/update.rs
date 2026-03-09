use super::{
    assert_invalid_value, assert_message, assert_rows, assert_type_mismatch, empty_context,
};
use crate::schema::Value;

#[test]
fn update_single_row_with_where_clause() {
    let mut context = empty_context();

    context
        .engine
        .execute("CREATE TABLE users (id INT, name VARCHAR(10), score FLOAT)")
        .unwrap();
    context
        .engine
        .execute("INSERT INTO users VALUES (1, 'alice', 3.5), (2, 'bob', 4.0)")
        .unwrap();

    assert_message(
        &mut context.engine,
        "UPDATE users SET name = 'bobby' WHERE id = 2",
        "1 row(s) updated in 'users'",
    );

    assert_rows(
        &mut context.engine,
        "SELECT id, name, score FROM users WHERE id = 2",
        &["id", "name", "score"],
        vec![vec![
            Value::Int(2),
            Value::Str("bobby".to_string()),
            Value::Float(4.0),
        ]],
    );
}

#[test]
fn update_multiple_rows_without_where_clause() {
    let mut context = empty_context();

    context
        .engine
        .execute("CREATE TABLE users (id INT, name VARCHAR(10), score FLOAT)")
        .unwrap();
    context
        .engine
        .execute("INSERT INTO users VALUES (1, 'alice', 3.5), (2, 'bob', 4.0)")
        .unwrap();

    assert_message(
        &mut context.engine,
        "UPDATE users SET score = 5.0",
        "2 row(s) updated in 'users'",
    );

    assert_rows(
        &mut context.engine,
        "SELECT * FROM users",
        &["id", "name", "score"],
        vec![
            vec![
                Value::Int(1),
                Value::Str("alice".to_string()),
                Value::Float(5.0),
            ],
            vec![
                Value::Int(2),
                Value::Str("bob".to_string()),
                Value::Float(5.0),
            ],
        ],
    );
}

#[test]
fn update_type_mismatch_fails() {
    let mut context = empty_context();

    context
        .engine
        .execute("CREATE TABLE users (id INT, name VARCHAR(10))")
        .unwrap();
    context
        .engine
        .execute("INSERT INTO users VALUES (1, 'alice')")
        .unwrap();

    assert_type_mismatch(
        &mut context.engine,
        "UPDATE users SET id = 'bad' WHERE id = 1",
        "id",
    );
}

#[test]
fn update_where_matches_no_rows() {
    let mut context = empty_context();

    context
        .engine
        .execute("CREATE TABLE users (id INT, name VARCHAR(10))")
        .unwrap();
    context
        .engine
        .execute("INSERT INTO users VALUES (1, 'alice'), (2, 'bob')")
        .unwrap();

    assert_message(
        &mut context.engine,
        "UPDATE users SET name = 'nobody' WHERE id = 99",
        "0 row(s) updated in 'users'",
    );

    assert_rows(
        &mut context.engine,
        "SELECT * FROM users",
        &["id", "name"],
        vec![
            vec![Value::Int(1), Value::Str("alice".to_string())],
            vec![Value::Int(2), Value::Str("bob".to_string())],
        ],
    );
}

#[test]
fn update_not_null_column_to_null_fails() {
    let mut context = empty_context();

    context
        .engine
        .execute("CREATE TABLE users (id INT NOT NULL, name VARCHAR(10) NOT NULL)")
        .unwrap();
    context
        .engine
        .execute("INSERT INTO users VALUES (1, 'alice'), (2, 'bob')")
        .unwrap();

    assert_invalid_value(
        &mut context.engine,
        "UPDATE users SET name = NULL WHERE id = 1",
        "name",
        |reason| reason.to_ascii_lowercase().contains("not null"),
    );

    assert_rows(
        &mut context.engine,
        "SELECT * FROM users",
        &["id", "name"],
        vec![
            vec![Value::Int(1), Value::Str("alice".to_string())],
            vec![Value::Int(2), Value::Str("bob".to_string())],
        ],
    );
}
