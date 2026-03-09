use std::path::{Path, PathBuf};

/// Filesystem layout helper for the storage subsystem.
///
/// This type centralizes path construction for all on-disk artifacts used by the
/// mini DBMS so higher-level storage components do not need to duplicate path
/// conventions.
///
/// Layout under the configured root directory:
/// - `schema.json`            : database schema metadata
/// - `wal.log`                : write-ahead log
/// - `<table>.jsonl`          : table row storage
/// - `<table>.jsonl.tmp`      : temporary rewrite file for atomic table updates
#[derive(Debug, Clone)]
pub struct StorageLayout {
    root: PathBuf,
}

impl StorageLayout {
    /// Create a new layout rooted at the given directory.
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
        }
    }

    /// Return the storage root directory.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Return the path of the schema metadata file.
    pub fn schema_path(&self) -> PathBuf {
        self.root.join("schema.json")
    }

    /// Return the path of the WAL file.
    pub fn wal_path(&self) -> PathBuf {
        self.root.join("wal.log")
    }

    /// Return the path of a table data file.
    pub fn table_path(&self, table: &str) -> PathBuf {
        self.root.join(format!("{table}.jsonl"))
    }

    /// Return the path of a temporary table rewrite file.
    pub fn temp_table_path(&self, table: &str) -> PathBuf {
        self.root.join(format!("{table}.jsonl.tmp"))
    }
}
