use super::{assert_message, empty_context};

// ── CREATE INDEX ────────────────────────────────────────────────────

#[test]
fn create_index_on_int_column() {
    let mut ctx = empty_context();
    ctx.engine
        .execute("CREATE TABLE users (id INT, name VARCHAR(10))")
        .unwrap();
    ctx.engine
        .execute("INSERT INTO users VALUES (1, 'alice'), (2, 'bob')")
        .unwrap();

    assert_message(
        &mut ctx.engine,
        "CREATE INDEX idx_users_id ON users (id)",
        "Index 'idx_users_id' created on 'id'",
    );
}

#[test]
fn create_index_rejects_duplicate() {
    let mut ctx = empty_context();
    ctx.engine
        .execute("CREATE TABLE users (id INT, name VARCHAR(10))")
        .unwrap();
    ctx.engine.execute("CREATE INDEX idx1 ON users (id)").unwrap();

    let err = ctx
        .engine
        .execute("CREATE INDEX idx1 ON users (name)")
        .unwrap_err();
    assert!(matches!(
        err,
        crate::error::DbError::IndexExists(ref s) if s == "idx1"
    ));
}

#[test]
fn create_index_if_not_exists_duplicate_is_ok() {
    let mut ctx = empty_context();
    ctx.engine
        .execute("CREATE TABLE users (id INT, name VARCHAR(10))")
        .unwrap();
    ctx.engine.execute("CREATE INDEX idx1 ON users (id)").unwrap();

    // Second time with IF NOT EXISTS should just produce a message.
    assert_message(
        &mut ctx.engine,
        "CREATE INDEX IF NOT EXISTS idx1 ON users (name)",
        "Index 'idx1' already exists",
    );
}

#[test]
fn create_index_on_nonexistent_table_fails() {
    let mut ctx = empty_context();
    let err = ctx
        .engine
        .execute("CREATE INDEX idx1 ON ghosts (id)")
        .unwrap_err();
    assert!(matches!(
        err,
        crate::error::DbError::TableNotFound(ref s) if s == "ghosts"
    ));
}

#[test]
fn create_index_on_nonexistent_column_fails() {
    let mut ctx = empty_context();
    ctx.engine
        .execute("CREATE TABLE users (id INT, name VARCHAR(10))")
        .unwrap();
    let err = ctx
        .engine
        .execute("CREATE INDEX idx1 ON users (age)")
        .unwrap_err();
    assert!(matches!(
        err,
        crate::error::DbError::ColumnNotFound(ref s) if s == "age"
    ));
}

#[test]
fn create_index_on_str_column() {
    let mut ctx = empty_context();
    ctx.engine
        .execute("CREATE TABLE users (id INT, name VARCHAR(10))")
        .unwrap();
    ctx.engine
        .execute("INSERT INTO users VALUES (1, 'alice'), (2, 'bob')")
        .unwrap();

    assert_message(
        &mut ctx.engine,
        "CREATE INDEX idx_name ON users (name)",
        "Index 'idx_name' created on 'name'",
    );
}

#[test]
fn create_index_persists_in_schema() {
    let mut ctx = empty_context();
    ctx.engine
        .execute("CREATE TABLE users (id INT, name VARCHAR(10))")
        .unwrap();
    ctx.engine
        .execute("CREATE INDEX idx_users_id ON users (id)")
        .unwrap();

    // Operations after index creation still work normally.
    ctx.engine
        .execute("INSERT INTO users VALUES (1, 'alice')")
        .unwrap();
    ctx.engine
        .execute("INSERT INTO users VALUES (2, 'bob')")
        .unwrap();
}

// ── DROP INDEX ──────────────────────────────────────────────────────

#[test]
fn drop_index_succeeds() {
    let mut ctx = empty_context();
    ctx.engine
        .execute("CREATE TABLE users (id INT, name VARCHAR(10))")
        .unwrap();
    ctx.engine.execute("CREATE INDEX idx1 ON users (id)").unwrap();

    assert_message(
        &mut ctx.engine,
        "DROP INDEX idx1",
        "Index 'idx1' dropped",
    );
}

#[test]
fn drop_index_rejects_nonexistent() {
    let mut ctx = empty_context();
    let err = ctx.engine.execute("DROP INDEX ghosts").unwrap_err();
    assert!(matches!(
        err,
        crate::error::DbError::IndexNotFound(ref s) if s == "ghosts"
    ));
}

#[test]
fn drop_index_if_exists_nonexistent_is_ok() {
    let mut ctx = empty_context();
    assert_message(
        &mut ctx.engine,
        "DROP INDEX IF EXISTS ghosts",
        "Index 'ghosts' does not exist",
    );
}

#[test]
fn create_index_multi_column_rejected() {
    let mut ctx = empty_context();
    ctx.engine
        .execute("CREATE TABLE users (id INT, name VARCHAR(10), age INT)")
        .unwrap();
    let err = ctx
        .engine
        .execute("CREATE INDEX idx1 ON users (id, name)")
        .unwrap_err();
    assert!(matches!(err, crate::error::DbError::Syntax(_)));
}
