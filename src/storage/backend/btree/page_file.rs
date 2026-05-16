//! Page-level I/O backed by raw file operations.
//!
//! This is the page storage provider for BTree index files.
//! Unlike [`JsonBackend`], it does no JSON serialization —
//! it only reads and writes fixed-size 4096-byte pages.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::error::DbResult;
use crate::schema::DatabaseSchema;

use super::super::{PageStorage, StorageBackend, StoredRow};

/// Stateless page-level I/O handler.
#[derive(Debug, Clone, Default)]
pub struct PageFile;

impl StorageBackend for PageFile {
    fn schema_path(&self, _root: &Path) -> PathBuf {
        unimplemented!()
    }
    fn wal_path(&self, _root: &Path) -> PathBuf {
        unimplemented!()
    }
    fn table_path(&self, _root: &Path, _table: &str) -> PathBuf {
        unimplemented!()
    }
    fn index_path(&self, root: &Path, index_name: &str) -> PathBuf {
        root.join(format!("idx_{index_name}.ndx"))
    }
    fn load_schema(&self, _path: &Path) -> DbResult<DatabaseSchema> {
        unimplemented!()
    }
    fn save_schema(&self, _path: &Path, _schema: &DatabaseSchema) -> DbResult<()> {
        unimplemented!()
    }
    fn scan_rows<F>(&self, _path: &Path, _func: F) -> DbResult<()>
    where
        F: FnMut(&StoredRow) -> DbResult<()>,
    {
        unimplemented!()
    }
    fn append_row(&self, _path: &Path, _row: &StoredRow) -> DbResult<()> {
        unimplemented!()
    }
    fn rewrite_rows(&self, _path: &Path, _rows: &[StoredRow]) -> DbResult<()> {
        unimplemented!()
    }
    fn read_rows_by_id<F>(&self, _path: &Path, _row_ids: &[u64], _func: F) -> DbResult<()>
    where
        F: FnMut(&StoredRow) -> DbResult<()>,
    {
        unimplemented!()
    }
    fn create_file(&self, _path: &Path) -> DbResult<()> {
        unimplemented!()
    }
    fn remove_file(&self, _path: &Path) -> DbResult<()> {
        unimplemented!()
    }
    fn rename_file(&self, _from: &Path, _to: &Path) -> DbResult<()> {
        unimplemented!()
    }
    fn file_exists(&self, path: &Path) -> bool {
        path.exists()
    }
    fn create_dir_all(&self, path: &Path) -> DbResult<()> {
        std::fs::create_dir_all(path)?;
        Ok(())
    }
}

impl PageStorage for PageFile {
    fn page_size(&self) -> usize {
        4096
    }

    fn read_page(&self, path: &Path, page_num: u64) -> DbResult<Vec<u8>> {
        let mut file = File::open(path)?;
        let offset = page_num * 4096;
        file.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0u8; 4096];
        file.read_exact(&mut buf)?;
        Ok(buf)
    }

    fn write_page(&self, path: &Path, page_num: u64, data: &[u8]) -> DbResult<()> {
        let mut file = OpenOptions::new().create(true).write(true).open(path)?;
        let offset = page_num * 4096;
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(data)?;
        file.flush()?;
        Ok(())
    }

    fn num_pages(&self, path: &Path) -> DbResult<u64> {
        if !path.exists() {
            return Ok(0);
        }
        Ok(path.metadata()?.len() / 4096)
    }
}
