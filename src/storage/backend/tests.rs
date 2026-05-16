//! Unit tests for page-level I/O on JsonBackend.

use std::io::Write;
use tempfile::TempDir;

use crate::storage::backend::{JsonBackend, StorageBackend};

fn temp_path(dir: &TempDir, name: &str) -> std::path::PathBuf {
    dir.path().join(name)
}

#[test]
fn read_write_page_roundtrip() {
    let backend = JsonBackend;
    let dir = TempDir::new().unwrap();
    let path = temp_path(&dir, "pages.bin");

    // Write page 0 and page 2 (skip page 1)
    let data0 = vec![0xAAu8; 4096];
    let data2 = vec![0xBBu8; 4096];
    backend.write_page(&path, 0, &data0).unwrap();
    backend.write_page(&path, 2, &data2).unwrap();

    // Read back
    let read0 = backend.read_page(&path, 0).unwrap();
    let read2 = backend.read_page(&path, 2).unwrap();
    assert_eq!(read0, data0);
    assert_eq!(read2, data2);
    assert_eq!(read0.len(), 4096);
    assert_eq!(read2.len(), 4096);
}

#[test]
fn num_pages_empty_file() {
    let backend = JsonBackend;
    let dir = TempDir::new().unwrap();
    let path = temp_path(&dir, "empty.bin");
    assert_eq!(backend.num_pages(&path).unwrap(), 0);
}

#[test]
fn num_pages_after_write() {
    let backend = JsonBackend;
    let dir = TempDir::new().unwrap();
    let path = temp_path(&dir, "data.bin");

    backend.write_page(&path, 0, &vec![0; 4096]).unwrap();
    assert_eq!(backend.num_pages(&path).unwrap(), 1);

    backend.write_page(&path, 3, &vec![0; 4096]).unwrap();
    assert_eq!(backend.num_pages(&path).unwrap(), 4);
}

#[test]
fn num_pages_partial_page() {
    let backend = JsonBackend;
    let dir = TempDir::new().unwrap();
    let path = temp_path(&dir, "partial.bin");

    // Write less than a full page
    let mut file = std::fs::File::create(&path).unwrap();
    file.write_all(&[1u8; 5000]).unwrap();
    drop(file);

    // 5000 / 4096 = 1 page (partial pages don't count)
    assert_eq!(backend.num_pages(&path).unwrap(), 1);
}

#[test]
fn read_page_past_end_is_error() {
    let backend = JsonBackend;
    let dir = TempDir::new().unwrap();
    let path = temp_path(&dir, "small.bin");

    backend.write_page(&path, 0, &vec![0; 4096]).unwrap();
    // File has 1 page, reading page 1 should fail (file too short)
    let result = backend.read_page(&path, 1);
    assert!(result.is_err());
}

#[test]
fn write_page_grows_file() {
    let backend = JsonBackend;
    let dir = TempDir::new().unwrap();
    let path = temp_path(&dir, "grow.bin");

    // Write page 0
    backend.write_page(&path, 0, &vec![0xFF; 4096]).unwrap();
    // Write page 5 (skips pages 1-4, file should extend)
    backend.write_page(&path, 5, &vec![0xEE; 4096]).unwrap();

    assert_eq!(backend.num_pages(&path).unwrap(), 6);
    let page5 = backend.read_page(&path, 5).unwrap();
    assert_eq!(page5, vec![0xEE; 4096]);
}

#[test]
fn page_size_is_4096() {
    let backend = JsonBackend;
    assert_eq!(backend.page_size(), 4096);
}
