use super::{assert_message, assert_table_not_found, empty_context};

#[test]
fn drop_table_removes_schema_and_data() {
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
        "DROP TABLE users",
        "Table 'users' dropped",
    );
    assert_table_not_found(&mut context.engine, "SELECT * FROM users", "users");
}

#[test]
fn drop_table_if_exists_allows_missing_table() {
    let mut context = empty_context();

    assert_message(
        &mut context.engine,
        "DROP TABLE IF EXISTS users",
        "Table 'users' dropped",
    );
}

#[test]
fn drop_table_without_if_exists_rejects_missing_table() {
    let mut context = empty_context();

    assert_table_not_found(&mut context.engine, "DROP TABLE users", "users");
}
