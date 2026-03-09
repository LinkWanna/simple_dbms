use super::{
    assert_message, assert_rows, assert_syntax_error, assert_table_not_found, empty_context,
    engine_at, wal_path,
};
use crate::schema::Value;
use tempfile::tempdir;

#[test]
fn begin_commit_persists_inserted_rows() {
    let mut context = empty_context();

    context
        .engine
        .execute("CREATE TABLE users (id INT, name VARCHAR(10))")
        .unwrap();

    assert_message(&mut context.engine, "BEGIN", "Transaction started");

    context
        .engine
        .execute("INSERT INTO users VALUES (1, 'alice'), (2, 'bob')")
        .unwrap();

    assert_message(&mut context.engine, "COMMIT", "Transaction committed");

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
fn rollback_restores_inserted_rows() {
    let mut context = empty_context();

    context
        .engine
        .execute("CREATE TABLE users (id INT, name VARCHAR(10))")
        .unwrap();

    context.engine.execute("BEGIN").unwrap();
    context
        .engine
        .execute("INSERT INTO users VALUES (1, 'alice'), (2, 'bob')")
        .unwrap();

    assert_message(&mut context.engine, "ROLLBACK", "Transaction rolled back");

    assert_rows(
        &mut context.engine,
        "SELECT * FROM users",
        &["id", "name"],
        vec![],
    );
}

#[test]
fn rollback_restores_created_table_removal() {
    let mut context = empty_context();

    context.engine.execute("BEGIN").unwrap();
    context
        .engine
        .execute("CREATE TABLE users (id INT, name VARCHAR(10))")
        .unwrap();

    assert_message(&mut context.engine, "ROLLBACK", "Transaction rolled back");
    assert_table_not_found(&mut context.engine, "SELECT * FROM users", "users");
}

#[test]
fn rollback_restores_dropped_table() {
    let mut context = empty_context();

    context
        .engine
        .execute("CREATE TABLE users (id INT, name VARCHAR(10))")
        .unwrap();
    context
        .engine
        .execute("INSERT INTO users VALUES (1, 'alice')")
        .unwrap();

    context.engine.execute("BEGIN").unwrap();
    context.engine.execute("DROP TABLE users").unwrap();

    assert_message(&mut context.engine, "ROLLBACK", "Transaction rolled back");

    assert_rows(
        &mut context.engine,
        "SELECT * FROM users",
        &["id", "name"],
        vec![vec![Value::Int(1), Value::Str("alice".to_string())]],
    );
}

#[test]
fn begin_inside_active_transaction_is_rejected() {
    let mut context = empty_context();

    context.engine.execute("BEGIN").unwrap();

    assert_syntax_error(
        &mut context.engine,
        "BEGIN",
        "nested transactions are not supported yet",
    );
}

#[test]
fn commit_without_active_transaction_is_rejected() {
    let mut context = empty_context();

    assert_syntax_error(&mut context.engine, "COMMIT", "no active transaction");
}

#[test]
fn rollback_without_active_transaction_is_rejected() {
    let mut context = empty_context();

    assert_syntax_error(&mut context.engine, "ROLLBACK", "no active transaction");
}

#[test]
fn rollback_with_savepoint_name_is_rejected() {
    let mut context = empty_context();

    context.engine.execute("BEGIN").unwrap();

    assert_syntax_error(
        &mut context.engine,
        "ROLLBACK TO sp1",
        "ROLLBACK TO savepoint is not supported yet",
    );
}

#[test]
fn savepoint_and_release_are_not_supported_yet() {
    let mut context = empty_context();

    assert_syntax_error(
        &mut context.engine,
        "SAVEPOINT sp1",
        "SAVEPOINT is not supported yet",
    );
    assert_syntax_error(
        &mut context.engine,
        "RELEASE sp1",
        "RELEASE SAVEPOINT is not supported yet",
    );
}

#[test]
fn engine_restart_recovers_uncommitted_insert_from_wal() {
    let dir = tempdir().unwrap();

    {
        let mut engine = engine_at(dir.path());
        engine
            .execute("CREATE TABLE users (id INT, name VARCHAR(10))")
            .unwrap();
        engine.execute("BEGIN").unwrap();
        engine
            .execute("INSERT INTO users VALUES (1, 'alice'), (2, 'bob')")
            .unwrap();
    }

    assert!(wal_path(dir.path()).exists());

    let mut recovered_engine = engine_at(dir.path());

    assert_rows(
        &mut recovered_engine,
        "SELECT * FROM users",
        &["id", "name"],
        vec![],
    );

    assert!(!wal_path(dir.path()).exists());
}

#[test]
fn engine_restart_recovers_uncommitted_create_table_from_wal() {
    let dir = tempdir().unwrap();

    {
        let mut engine = engine_at(dir.path());
        engine.execute("BEGIN").unwrap();
        engine
            .execute("CREATE TABLE users (id INT, name VARCHAR(10))")
            .unwrap();
    }

    assert!(wal_path(dir.path()).exists());

    let mut recovered_engine = engine_at(dir.path());

    assert_table_not_found(&mut recovered_engine, "SELECT * FROM users", "users");

    assert!(!wal_path(dir.path()).exists());
}

#[test]
fn engine_restart_recovers_uncommitted_drop_table_from_wal() {
    let dir = tempdir().unwrap();

    {
        let mut engine = engine_at(dir.path());
        engine
            .execute("CREATE TABLE users (id INT, name VARCHAR(10))")
            .unwrap();
        engine
            .execute("INSERT INTO users VALUES (1, 'alice')")
            .unwrap();
        engine.execute("BEGIN").unwrap();
        engine.execute("DROP TABLE users").unwrap();
    }

    assert!(wal_path(dir.path()).exists());

    let mut recovered_engine = engine_at(dir.path());

    assert_rows(
        &mut recovered_engine,
        "SELECT * FROM users",
        &["id", "name"],
        vec![vec![Value::Int(1), Value::Str("alice".to_string())]],
    );

    assert!(!wal_path(dir.path()).exists());
}
