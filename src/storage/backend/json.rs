//! JSON-based storage backend.
//!
//! On-disk layout:
//! - `schema.json` — database schema metadata (pretty-printed JSON)
//! - `wal.log` — write-ahead log (JSONL)
//! - `<table>.jsonl` — table row storage (JSONL, one `StoredRow` per line)
//! - `<table>.jsonl.tmp` — temporary file for atomic table rewrites

use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use crate::error::DbResult;
use crate::schema::DatabaseSchema;

use super::{StorageBackend, StoredRow};

/// Backend that persists everything as human-readable JSON / JSONL files.
///
/// This is the default backend for the teaching DBMS. Data files can be
/// inspected with any text editor.
#[derive(Debug, Clone, Default)]
pub struct JsonBackend;

impl StorageBackend for JsonBackend {
    // ── Paths ─────────────────────────────────────────────────────

    fn schema_path(&self, root: &Path) -> PathBuf {
        root.join("schema.json")
    }

    fn wal_path(&self, root: &Path) -> PathBuf {
        root.join("wal.log")
    }

    fn table_path(&self, root: &Path, table: &str) -> PathBuf {
        root.join(format!("{table}.jsonl"))
    }

    // ── Schema I/O ────────────────────────────────────────────────

    fn load_schema(&self, path: &Path) -> DbResult<DatabaseSchema> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        Ok(serde_json::from_reader(reader)?)
    }

    fn save_schema(&self, path: &Path, schema: &DatabaseSchema) -> DbResult<()> {
        let file = File::create(path)?;
        serde_json::to_writer_pretty(file, schema)?;
        Ok(())
    }

    // ── Row I/O ───────────────────────────────────────────────────

    fn scan_rows<F>(&self, path: &Path, mut func: F) -> DbResult<()>
    where
        F: FnMut(&StoredRow) -> DbResult<()>,
    {
        let file = File::open(path)?;
        let reader = BufReader::new(file);

        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let row: StoredRow = serde_json::from_str(&line)?;
            func(&row)?;
        }

        Ok(())
    }

    fn append_row(&self, path: &Path, row: &StoredRow) -> DbResult<()> {
        let mut file = OpenOptions::new().create(false).append(true).open(path)?;
        let line = serde_json::to_string(row)?;
        file.write_all(line.as_bytes())?;
        file.write_all(b"\n")?;
        Ok(())
    }

    fn rewrite_rows(&self, path: &Path, rows: &[StoredRow]) -> DbResult<()> {
        let temp_path = path.with_extension("jsonl.tmp");
        {
            let mut file = File::create(&temp_path)?;
            for row in rows {
                let line = serde_json::to_string(row)?;
                file.write_all(line.as_bytes())?;
                file.write_all(b"\n")?;
            }
            file.flush()?;
        }
        fs::rename(temp_path, path)?;
        Ok(())
    }

    // ── File-system helpers ───────────────────────────────────────

    fn create_file(&self, path: &Path) -> DbResult<()> {
        File::create(path)?;
        Ok(())
    }

    fn remove_file(&self, path: &Path) -> DbResult<()> {
        fs::remove_file(path)?;
        Ok(())
    }

    fn rename_file(&self, from: &Path, to: &Path) -> DbResult<()> {
        fs::rename(from, to)?;
        Ok(())
    }

    fn file_exists(&self, path: &Path) -> bool {
        path.exists()
    }

    fn create_dir_all(&self, path: &Path) -> DbResult<()> {
        fs::create_dir_all(path)?;
        Ok(())
    }
}
