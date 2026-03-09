use super::{
    assert_column_not_found, assert_message, assert_rows, assert_syntax_error,
    assert_table_not_found, empty_context,
};
use crate::schema::Value;

#[test]
fn alter_table_rename_table_preserves_rows() {
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
        "ALTER TABLE users RENAME TO customers",
        "Table 'users' renamed to 'customers'",
    );

    assert_table_not_found(&mut context.engine, "SELECT * FROM users", "users");

    assert_rows(
        &mut context.engine,
        "SELECT * FROM customers",
        &["id", "name"],
        vec![
            vec![Value::Int(1), Value::Str("alice".to_string())],
            vec![Value::Int(2), Value::Str("bob".to_string())],
        ],
    );
}

#[test]
fn alter_table_rename_column_updates_projection_and_where() {
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
        "ALTER TABLE users RENAME COLUMN name TO username",
        "Column 'name' renamed to 'username' in 'users'",
    );

    assert_rows(
        &mut context.engine,
        "SELECT id, username FROM users WHERE username = 'alice'",
        &["id", "username"],
        vec![vec![Value::Int(1), Value::Str("alice".to_string())]],
    );

    assert_column_not_found(&mut context.engine, "SELECT id, name FROM users", "name");
}

#[test]
fn alter_table_add_column_backfills_null_for_existing_rows() {
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
        "ALTER TABLE users ADD COLUMN score FLOAT",
        "Column 'score' added to 'users'",
    );

    assert_rows(
        &mut context.engine,
        "SELECT * FROM users",
        &["id", "name", "score"],
        vec![
            vec![Value::Int(1), Value::Str("alice".to_string()), Value::Null],
            vec![Value::Int(2), Value::Str("bob".to_string()), Value::Null],
        ],
    );
}

#[test]
fn alter_table_add_not_null_column_without_default_rejects_non_empty_table() {
    let mut context = empty_context();

    context
        .engine
        .execute("CREATE TABLE users (id INT, name VARCHAR(10))")
        .unwrap();
    context
        .engine
        .execute("INSERT INTO users VALUES (1, 'alice')")
        .unwrap();

    assert_syntax_error(
        &mut context.engine,
        "ALTER TABLE users ADD COLUMN score FLOAT NOT NULL",
        "cannot add a NOT NULL column without DEFAULT to a non-empty table",
    );
}

#[test]
fn alter_table_add_column_with_default_backfills_existing_rows() {
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
        "ALTER TABLE users ADD COLUMN score FLOAT DEFAULT 0.0",
        "Column 'score' added to 'users'",
    );

    assert_rows(
        &mut context.engine,
        "SELECT * FROM users",
        &["id", "name", "score"],
        vec![
            vec![
                Value::Int(1),
                Value::Str("alice".to_string()),
                Value::Float(0.0),
            ],
            vec![
                Value::Int(2),
                Value::Str("bob".to_string()),
                Value::Float(0.0),
            ],
        ],
    );
}

#[test]
fn alter_table_drop_column_removes_column_from_rows() {
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
        "ALTER TABLE users DROP COLUMN score",
        "Column 'score' dropped from 'users'",
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
fn alter_table_drop_last_column_is_rejected() {
    let mut context = empty_context();

    context
        .engine
        .execute("CREATE TABLE users (id INT)")
        .unwrap();

    assert_syntax_error(
        &mut context.engine,
        "ALTER TABLE users DROP COLUMN id",
        "cannot drop the last column of a table",
    );
}
