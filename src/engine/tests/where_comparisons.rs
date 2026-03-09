use super::{assert_id_rows, assert_rows, seed_users, seed_users_with_nullable_name};
use crate::schema::Value;

#[test]
fn select_where_equal_on_int() {
    let mut context = seed_users();

    assert_rows(
        &mut context.engine,
        "SELECT id, name FROM users WHERE id = 2",
        &["id", "name"],
        vec![vec![Value::Int(2), Value::Str("bob".to_string())]],
    );
}

#[test]
fn select_where_not_equal_on_int() {
    let mut context = seed_users();

    let result = context
        .engine
        .execute("SELECT id FROM users WHERE id != 2")
        .unwrap();

    assert_id_rows(result, &[1, 3, 4]);
}

#[test]
fn select_where_greater_than_on_int() {
    let mut context = seed_users();

    assert_rows(
        &mut context.engine,
        "SELECT id, name FROM users WHERE id > 2",
        &["id", "name"],
        vec![
            vec![Value::Int(3), Value::Str("carol".to_string())],
            vec![Value::Int(4), Value::Str("dave".to_string())],
        ],
    );
}

#[test]
fn select_where_greater_equal_on_float() {
    let mut context = seed_users();

    assert_rows(
        &mut context.engine,
        "SELECT name, score FROM users WHERE score >= 4.0",
        &["name", "score"],
        vec![
            vec![Value::Str("bob".to_string()), Value::Float(4.0)],
            vec![Value::Str("carol".to_string()), Value::Float(4.5)],
        ],
    );
}

#[test]
fn select_where_less_than_on_float() {
    let mut context = seed_users();

    assert_rows(
        &mut context.engine,
        "SELECT name, score FROM users WHERE score < 4.0",
        &["name", "score"],
        vec![
            vec![Value::Str("alice".to_string()), Value::Float(3.5)],
            vec![Value::Str("dave".to_string()), Value::Float(2.5)],
        ],
    );
}

#[test]
fn select_where_less_equal_on_int() {
    let mut context = seed_users();

    let result = context
        .engine
        .execute("SELECT id FROM users WHERE id <= 2")
        .unwrap();

    assert_id_rows(result, &[1, 2]);
}

#[test]
fn select_where_equal_on_string() {
    let mut context = seed_users();

    assert_rows(
        &mut context.engine,
        "SELECT id, name FROM users WHERE name = 'carol'",
        &["id", "name"],
        vec![vec![Value::Int(3), Value::Str("carol".to_string())]],
    );
}

#[test]
fn select_where_not_equal_on_string() {
    let mut context = seed_users();

    assert_rows(
        &mut context.engine,
        "SELECT name FROM users WHERE name != 'bob'",
        &["name"],
        vec![
            vec![Value::Str("alice".to_string())],
            vec![Value::Str("carol".to_string())],
            vec![Value::Str("dave".to_string())],
        ],
    );
}

#[test]
fn select_where_and_on_mixed_comparisons() {
    let mut context = seed_users();

    assert_rows(
        &mut context.engine,
        "SELECT id, name FROM users WHERE id >= 2 AND score < 4.5",
        &["id", "name"],
        vec![
            vec![Value::Int(2), Value::Str("bob".to_string())],
            vec![Value::Int(4), Value::Str("dave".to_string())],
        ],
    );
}

#[test]
fn select_where_or_on_mixed_comparisons() {
    let mut context = seed_users();

    let result = context
        .engine
        .execute("SELECT id FROM users WHERE id = 1 OR score >= 4.5")
        .unwrap();

    assert_id_rows(result, &[1, 3]);
}

#[test]
fn select_where_and_or_precedence() {
    let mut context = seed_users();

    let result = context
        .engine
        .execute("SELECT id FROM users WHERE id = 1 OR id = 2 AND score >= 4.0")
        .unwrap();

    assert_id_rows(result, &[1, 2]);
}

#[test]
fn select_where_grouped_and_or_without_parentheses() {
    let mut context = seed_users();

    let result = context
        .engine
        .execute("SELECT id FROM users WHERE id = 2 AND score >= 4.0 OR id = 1 AND score > 4.0")
        .unwrap();

    assert_id_rows(result, &[2]);
}

#[test]
fn select_where_grouped_boolean_expression_with_parentheses() {
    let mut context = seed_users();

    let result = context
        .engine
        .execute("SELECT id FROM users WHERE (id = 1 OR id = 2) AND score >= 4.0")
        .unwrap();

    assert_id_rows(result, &[2]);
}

#[test]
fn select_where_nested_grouped_boolean_expression() {
    let mut context = seed_users();

    let result = context
        .engine
        .execute(
            "SELECT id FROM users WHERE (id = 1 OR id = 2) AND (score >= 4.0 OR name = 'alice')",
        )
        .unwrap();

    assert_id_rows(result, &[1, 2]);
}

#[test]
fn select_where_not_on_simple_predicate() {
    let mut context = seed_users();

    let result = context
        .engine
        .execute("SELECT id FROM users WHERE NOT id = 2")
        .unwrap();

    assert_id_rows(result, &[1, 3, 4]);
}

#[test]
fn select_where_not_on_grouped_boolean_expression() {
    let mut context = seed_users();

    let result = context
        .engine
        .execute("SELECT id FROM users WHERE NOT (id = 1 OR score >= 4.5)")
        .unwrap();

    assert_id_rows(result, &[2, 4]);
}

#[test]
fn select_where_not_with_and_expression() {
    let mut context = seed_users();

    let result = context
        .engine
        .execute("SELECT id FROM users WHERE NOT (id >= 2 AND score < 4.5)")
        .unwrap();

    assert_id_rows(result, &[1, 3]);
}

#[test]
fn select_where_is_null_matches_null_rows() {
    let mut context = seed_users_with_nullable_name();

    let result = context
        .engine
        .execute("SELECT id FROM users WHERE name IS NULL")
        .unwrap();

    assert_id_rows(result, &[2, 4]);
}

#[test]
fn select_where_is_not_null_matches_non_null_rows() {
    let mut context = seed_users_with_nullable_name();

    let result = context
        .engine
        .execute("SELECT id FROM users WHERE name IS NOT NULL")
        .unwrap();

    assert_id_rows(result, &[1, 3]);
}

#[test]
fn select_where_is_null_with_boolean_composition() {
    let mut context = seed_users_with_nullable_name();

    let result = context
        .engine
        .execute("SELECT id FROM users WHERE name IS NULL AND score >= 4.0")
        .unwrap();

    assert_id_rows(result, &[2]);
}

#[test]
fn select_where_not_on_is_null_expression() {
    let mut context = seed_users_with_nullable_name();

    let result = context
        .engine
        .execute("SELECT id FROM users WHERE NOT (name IS NULL)")
        .unwrap();

    assert_id_rows(result, &[1, 3]);
}
