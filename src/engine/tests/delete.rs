use super::{assert_message, assert_rows, empty_context};
use crate::schema::Value;

#[test]
fn delete_matching_rows_keeps_non_matching_rows() {
    let mut context = empty_context();

    context
        .engine
        .execute("CREATE TABLE users (id INT, name VARCHAR(10), score FLOAT)")
        .unwrap();
    context
        .engine
        .execute("INSERT INTO users VALUES (1, 'alice', 3.5), (2, 'bob', 4.0), (3, 'carol', 4.5)")
        .unwrap();

    assert_message(
        &mut context.engine,
        "DELETE FROM users WHERE id = 2",
        "1 row(s) deleted from 'users'",
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
                Value::Int(3),
                Value::Str("carol".to_string()),
                Value::Float(4.5),
            ],
        ],
    );
}

#[test]
fn delete_without_match_keeps_all_rows() {
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
        "DELETE FROM users WHERE id = 99",
        "0 row(s) deleted from 'users'",
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
fn delete_all_rows_without_where() {
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
        "DELETE FROM users",
        "2 row(s) deleted from 'users'",
    );

    assert_rows(
        &mut context.engine,
        "SELECT * FROM users",
        &["id", "name"],
        vec![],
    );
}
