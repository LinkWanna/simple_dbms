use super::{assert_rows, empty_context};
use crate::schema::Value;

#[test]
fn select_star_returns_all_columns() {
    let mut context = empty_context();

    context
        .engine
        .execute("CREATE TABLE users (id INT, name VARCHAR(10), score FLOAT)")
        .unwrap();
    context
        .engine
        .execute("INSERT INTO users VALUES (1, 'alice', 3.5), (2, 'bob', 4.0)")
        .unwrap();

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
fn select_mixed_star_and_column_preserves_projection_order() {
    let mut context = empty_context();

    context
        .engine
        .execute("CREATE TABLE users (id INT, name VARCHAR(10), score FLOAT)")
        .unwrap();
    context
        .engine
        .execute("INSERT INTO users VALUES (1, 'alice', 3.5), (2, 'bob', 4.0)")
        .unwrap();

    assert_rows(
        &mut context.engine,
        "SELECT name, * FROM users",
        &["name", "id", "name", "score"],
        vec![
            vec![
                Value::Str("alice".to_string()),
                Value::Int(1),
                Value::Str("alice".to_string()),
                Value::Float(3.5),
            ],
            vec![
                Value::Str("bob".to_string()),
                Value::Int(2),
                Value::Str("bob".to_string()),
                Value::Float(4.0),
            ],
        ],
    );
}

#[test]
fn select_repeated_star_repeats_all_columns() {
    let mut context = empty_context();

    context
        .engine
        .execute("CREATE TABLE users (id INT, name VARCHAR(10), score FLOAT)")
        .unwrap();
    context
        .engine
        .execute("INSERT INTO users VALUES (1, 'alice', 3.5)")
        .unwrap();

    assert_rows(
        &mut context.engine,
        "SELECT *, * FROM users",
        &["id", "name", "score", "id", "name", "score"],
        vec![vec![
            Value::Int(1),
            Value::Str("alice".to_string()),
            Value::Float(3.5),
            Value::Int(1),
            Value::Str("alice".to_string()),
            Value::Float(3.5),
        ]],
    );
}
