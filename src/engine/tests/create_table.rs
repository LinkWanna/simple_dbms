use super::{assert_message, assert_rows, assert_unique_constraint_error, empty_context};
use crate::schema::Value;

#[test]
fn create_insert_select_flow() {
    let mut context = empty_context();

    context
        .engine
        .execute("CREATE TABLE users (id INT, name VARCHAR(10))")
        .expect("create table failed");
    context
        .engine
        .execute("INSERT INTO users VALUES (1, 'alice'), (2, 'bob')")
        .expect("insert failed");

    assert_rows(
        &mut context.engine,
        "SELECT id, name FROM users WHERE id = 2",
        &["id", "name"],
        vec![vec![Value::Int(2), Value::Str("bob".to_string())]],
    );
}

#[test]
fn create_table_with_not_null_allows_normal_insert() {
    let mut context = empty_context();

    context
        .engine
        .execute("CREATE TABLE users (id INT NOT NULL, name VARCHAR(10) NOT NULL)")
        .expect("create table with not null failed");

    assert_message(
        &mut context.engine,
        "INSERT INTO users VALUES (1, 'alice')",
        "1 row(s) inserted into 'users'",
    );

    assert_rows(
        &mut context.engine,
        "SELECT id, name FROM users WHERE id = 1",
        &["id", "name"],
        vec![vec![Value::Int(1), Value::Str("alice".to_string())]],
    );
}

#[test]
fn create_table_with_unique_column_allows_distinct_inserts() {
    let mut context = empty_context();

    context
        .engine
        .execute("CREATE TABLE users (id INT UNIQUE, name VARCHAR(10))")
        .expect("create table with unique failed");

    context
        .engine
        .execute("INSERT INTO users VALUES (1, 'alice')")
        .expect("first insert failed");
    context
        .engine
        .execute("INSERT INTO users VALUES (2, 'bob')")
        .expect("second insert failed");

    assert_rows(
        &mut context.engine,
        "SELECT id, name FROM users WHERE id >= 1",
        &["id", "name"],
        vec![
            vec![Value::Int(1), Value::Str("alice".to_string())],
            vec![Value::Int(2), Value::Str("bob".to_string())],
        ],
    );
}

#[test]
fn unique_column_rejects_duplicate_insert() {
    let mut context = empty_context();

    context
        .engine
        .execute("CREATE TABLE users (id INT UNIQUE, name VARCHAR(10))")
        .expect("create table with unique failed");

    context
        .engine
        .execute("INSERT INTO users VALUES (1, 'alice')")
        .expect("first insert failed");

    assert_unique_constraint_error(
        &mut context.engine,
        "INSERT INTO users VALUES (1, 'bob')",
        "id",
    );
}

#[test]
fn unique_varchar_column_rejects_duplicate_insert() {
    let mut context = empty_context();

    context
        .engine
        .execute("CREATE TABLE users (id INT, email VARCHAR(20) UNIQUE)")
        .expect("create table with unique varchar failed");

    context
        .engine
        .execute("INSERT INTO users VALUES (1, 'a@example.com')")
        .expect("first insert failed");

    assert_unique_constraint_error(
        &mut context.engine,
        "INSERT INTO users VALUES (2, 'a@example.com')",
        "email",
    );
}

#[test]
fn unique_column_allows_multiple_null_values() {
    let mut context = empty_context();

    context
        .engine
        .execute("CREATE TABLE users (id INT, email VARCHAR(20) UNIQUE)")
        .expect("create table with unique nullable column failed");

    context
        .engine
        .execute("INSERT INTO users VALUES (1, NULL)")
        .expect("first null insert failed");
    context
        .engine
        .execute("INSERT INTO users VALUES (2, NULL)")
        .expect("second null insert failed");

    assert_rows(
        &mut context.engine,
        "SELECT id, email FROM users WHERE id >= 1",
        &["id", "email"],
        vec![
            vec![Value::Int(1), Value::Null],
            vec![Value::Int(2), Value::Null],
        ],
    );
}
