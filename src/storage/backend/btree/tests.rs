use tempfile::TempDir;

use crate::storage::backend::JsonBackend;
use super::BTree;

fn temp_dir() -> TempDir { TempDir::new().unwrap() }

fn temp_path(dir: &TempDir, name: &str) -> std::path::PathBuf {
    dir.path().join(name)
}

#[test]
fn create_and_search_empty() {
    let dir = temp_dir();
    let path = temp_path(&dir, "test.idx");
    let bt = BTree::create(JsonBackend, &path).unwrap();
    assert_eq!(bt.search(42).unwrap(), None);
}

#[test]
fn insert_and_search_one() {
    let dir = temp_dir();
    let path = temp_path(&dir, "test.idx");
    let mut bt = BTree::create(JsonBackend, &path).unwrap();
    bt.insert(42, 100).unwrap();
    assert_eq!(bt.search(42).unwrap(), Some(100));
    assert_eq!(bt.search(41).unwrap(), None);
    assert_eq!(bt.search(43).unwrap(), None);
}

#[test]
fn insert_and_search_many() {
    let dir = temp_dir();
    let path = temp_path(&dir, "test.idx");
    let mut bt = BTree::create(JsonBackend, &path).unwrap();
    for i in 0..100 {
        bt.insert(i, (i * 10) as u64).unwrap();
    }
    for i in 0..100 {
        assert_eq!(bt.search(i).unwrap(), Some((i * 10) as u64));
    }
}

#[test]
fn insert_duplicate_is_error() {
    let dir = temp_dir();
    let path = temp_path(&dir, "test.idx");
    let mut bt = BTree::create(JsonBackend, &path).unwrap();
    bt.insert(1, 10).unwrap();
    assert!(bt.insert(1, 20).is_err());
}

#[test]
fn range_scan() {
    let dir = temp_dir();
    let path = temp_path(&dir, "test.idx");
    let mut bt = BTree::create(JsonBackend, &path).unwrap();
    for i in 0..50 {
        bt.insert(i, (i * 2) as u64).unwrap();
    }
    let results = bt.range_scan(10, 20).unwrap();
    assert_eq!(results.len(), 11);
    for (i, row_id) in results.iter().enumerate() {
        assert_eq!(*row_id, ((10 + i) * 2) as u64);
    }
}

#[test]
fn reopen_persists_data() {
    let dir = temp_dir();
    let path = temp_path(&dir, "test.idx");
    {
        let mut bt = BTree::create(JsonBackend, &path).unwrap();
        bt.insert(7, 777).unwrap();
        bt.insert(3, 333).unwrap();
        bt.insert(99, 999).unwrap();
    }
    let bt = BTree::open(JsonBackend, &path).unwrap();
    assert_eq!(bt.search(7).unwrap(), Some(777));
    assert_eq!(bt.search(3).unwrap(), Some(333));
    assert_eq!(bt.search(99).unwrap(), Some(999));
}

#[test]
fn leaf_split_triggered() {
    let dir = temp_dir();
    let path = temp_path(&dir, "test.idx");
    let mut bt = BTree::create(JsonBackend, &path).unwrap();
    for i in 0..300 {
        bt.insert(i, (i * 10) as u64).unwrap();
    }
    for i in 0..300 {
        assert_eq!(bt.search(i).unwrap(), Some((i * 10) as u64));
    }
}

#[test]
fn range_scan_post_split() {
    let dir = temp_dir();
    let path = temp_path(&dir, "test.idx");
    let mut bt = BTree::create(JsonBackend, &path).unwrap();
    for i in 0..300 {
        bt.insert(i, (i * 100) as u64).unwrap();
    }
    let results = bt.range_scan(50, 60).unwrap();
    assert_eq!(results.len(), 11);
    for (offset, row_id) in results.iter().enumerate() {
        assert_eq!(*row_id, ((50 + offset) * 100) as u64);
    }
}
