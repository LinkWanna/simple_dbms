//! Integration tests for StorageImpl<BTreeBackend> — verify the full
//! storage facade works correctly with the B-Tree backend.

use crate::error::DbError;
use crate::storage::{StorageImpl, StoredRow};
use crate::storage::backend::BTreeBackend;
use tempfile::TempDir;

struct TestContext {
    _dir: TempDir, // keep alive until test ends
    storage: StorageImpl<BTreeBackend>,
}

fn temp_storage() -> TestContext {
    let dir = TempDir::new().unwrap();
    let storage = StorageImpl::new(dir.path()).unwrap();
    TestContext {
        _dir: dir,
        storage,
    }
}

/// Helper: create a test row with row_id and one Int value.
fn test_row(row_id: u64, val: i64) -> StoredRow {
    use crate::schema::Value;
    StoredRow {
        row_id,
        values: vec![Value::Int(val)],
    }
}

// ── Test 1: create + scan empty table ───────────────────────────────

#[test]
fn test_create_and_scan_empty() {
    let ctx = temp_storage();
    ctx.storage.create_table_file("users").unwrap();

    let rows = ctx.storage.load_rows("users").unwrap();
    assert!(rows.is_empty());
}

// ── Test 2: append rows and scan back ───────────────────────────────

#[test]
fn test_append_and_scan() {
    let ctx = temp_storage();
    ctx.storage.create_table_file("users").unwrap();

    ctx.storage
        .append_stored_row("users", &test_row(1, 100))
        .unwrap();
    ctx.storage
        .append_stored_row("users", &test_row(2, 200))
        .unwrap();
    ctx.storage
        .append_stored_row("users", &test_row(3, 300))
        .unwrap();

    let rows = ctx.storage.load_rows("users").unwrap();
    assert_eq!(rows.len(), 3, "expected 3 rows after append");
    // BTree scans in row_id order (sorted by key)
    assert_eq!(rows[0].row_id, 1);
    assert_eq!(rows[1].row_id, 2);
    assert_eq!(rows[2].row_id, 3);
}

// ── Test 3: rewrite_rows replaces all data ──────────────────────────

#[test]
fn test_rewrite_rows() {
    let ctx = temp_storage();
    ctx.storage.create_table_file("users").unwrap();

    // Append 5 rows
    for i in 1i64..=5 {
        ctx.storage
            .append_stored_row("users", &test_row(i as u64, i * 10))
            .unwrap();
    }

    // Rewrite with only 2 rows
    ctx.storage
        .rewrite_rows("users", &[test_row(10, 111), test_row(20, 222)])
        .unwrap();

    let rows = ctx.storage.load_rows("users").unwrap();
    assert_eq!(rows.len(), 2, "expected 2 rows after rewrite");
    assert_eq!(rows[0].row_id, 10);
    assert_eq!(rows[1].row_id, 20);
}

// ── Test 4: table_file_exists reflects create/remove ────────────────

#[test]
fn test_table_file_exists() {
    let ctx = temp_storage();
    assert!(!ctx.storage.table_file_exists("orders"));

    ctx.storage.create_table_file("orders").unwrap();
    assert!(ctx.storage.table_file_exists("orders"));

    ctx.storage.remove_table_file("orders").unwrap();
    assert!(!ctx.storage.table_file_exists("orders"));
}

// ── Test 5: remove_table_file makes rows inaccessible ───────────────

#[test]
fn test_remove_table_file() {
    let ctx = temp_storage();
    ctx.storage.create_table_file("users").unwrap();
    ctx.storage
        .append_stored_row("users", &test_row(1, 42))
        .unwrap();

    ctx.storage.remove_table_file("users").unwrap();

    let err = ctx.storage.load_rows("users").unwrap_err();
    assert!(
        matches!(err, DbError::TableNotFound(_)),
        "expected TableNotFound after remove, got {err:?}"
    );
}

// ── Test 6: force_rewrite_rows creates table implicitly ─────────────

#[test]
fn test_force_rewrite_rows() {
    let ctx = temp_storage();
    // No explicit create_table_file — force_rewrite_rows must work anyway.
    ctx.storage
        .force_rewrite_rows("scores", &[test_row(5, 999)])
        .unwrap();

    let rows = ctx.storage.load_rows("scores").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_id, 5);
}
