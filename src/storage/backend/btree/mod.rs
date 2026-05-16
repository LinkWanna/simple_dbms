//! B-Tree index data structure + BTree-based storage backend.
//!
//! **BTree<B>** — A persistent B-Tree that stores `(key: i64, value: u64)`
//! pairs in fixed-size 4096-byte pages, backed by any [StorageBackend].
//!
//! **BTreeBackend** — A [StorageBackend] that stores table rows using a
//! B-Tree index (row_id → data file offset) plus a companion binary data
//! file.  On-disk per table:
//!   `<table>.idx` — B-Tree index (key=row_id, value=offset)
//!   `<table>.dat` — length-prefixed JSON rows

pub mod layout;

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::error::{DbError, DbResult};
use crate::schema::DatabaseSchema;

use self::layout::{Entry, LEAF, MIN_ENTRIES, PAGE_SIZE, Page};
use super::{JsonBackend, PageStorage, StorageBackend, StoredRow};

// ═══════════════════════════════════════════════════════════════════════
//  B-Tree index data structure
// ═══════════════════════════════════════════════════════════════════════

struct Superblock {
    root_page: u64,
}

impl Superblock {
    fn serialize(&self) -> Vec<u8> {
        let mut buf = vec![0u8; PAGE_SIZE];
        buf[0..8].copy_from_slice(&self.root_page.to_le_bytes());
        buf[8] = 0x42;
        buf
    }

    fn deserialize(data: &[u8]) -> DbResult<Self> {
        if data.len() < PAGE_SIZE || data[8] != 0x42 {
            return Err(DbError::StorageCorruption("invalid B-Tree superblock".into()));
        }
        let root_page = u64::from_le_bytes(data[0..8].try_into().unwrap());
        Ok(Superblock { root_page })
    }
}

/// A persistent B-Tree index backed by a [StorageBackend].
pub struct BTree<B: PageStorage + StorageBackend> {
    backend: B,
    path: PathBuf,
    root_page: u64,
}

impl<B: PageStorage + StorageBackend> BTree<B> {
    /// Create a new, empty B-Tree index file.
    pub fn create(backend: B, path: impl AsRef<Path>) -> DbResult<Self> {
        let path = path.as_ref().to_path_buf();
        let sb = Superblock { root_page: 1 };
        let root = Page::new_leaf();
        backend.write_page(&path, 0, &sb.serialize())?;
        backend.write_page(&path, 1, &root.serialize())?;
        Ok(BTree {
            backend,
            path,
            root_page: 1,
        })
    }

    /// Open an existing B-Tree index file.
    pub fn open(backend: B, path: impl AsRef<Path>) -> DbResult<Self> {
        let path = path.as_ref().to_path_buf();
        if !backend.file_exists(&path) {
            return Err(DbError::syntax(format!(
                "B-Tree file not found: {}",
                path.display()
            )));
        }
        let sb = Superblock::deserialize(&backend.read_page(&path, 0)?)?;
        Ok(BTree {
            backend,
            path,
            root_page: sb.root_page,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn read_page(&self, n: u64) -> DbResult<Page> {
        Page::deserialize(&self.backend.read_page(&self.path, n)?)
    }
    fn write_page(&self, n: u64, page: &Page) -> DbResult<()> {
        self.backend.write_page(&self.path, n, &page.serialize())
    }
    fn write_superblock(&self) -> DbResult<()> {
        let sb = Superblock {
            root_page: self.root_page,
        };
        self.backend.write_page(&self.path, 0, &sb.serialize())
    }
    fn alloc_page(&self) -> DbResult<u64> {
        self.backend.num_pages(&self.path)
    }

    /// Search for a key. Returns the value if found.
    pub fn search(&self, key: i64) -> DbResult<Option<u64>> {
        self.search_in_page(self.root_page, key)
    }

    fn search_in_page(&self, page_num: u64, key: i64) -> DbResult<Option<u64>> {
        let page = self.read_page(page_num)?;
        if page.page_type == LEAF {
            for e in &page.entries {
                if e.key() == key {
                    return Ok(Some(e.row_id()));
                }
            }
            return Ok(None);
        }
        for e in &page.entries {
            if key < e.key() {
                if let Entry::Internal { left_child, .. } = e {
                    return self.search_in_page(*left_child, key);
                }
            }
        }
        self.search_in_page(page.rightmost_child, key)
    }

    /// Insert a key → value pair. Errors on duplicate.
    pub fn insert(&mut self, key: i64, value: u64) -> DbResult<()> {
        if let Some(split) = self.insert_recursive(self.root_page, key, value)? {
            let new_root = self.alloc_page()?;
            let mut root_page = Page::new_internal(split.right_page);
            root_page.entries.push(Entry::Internal {
                key: split.key,
                row_id: split.value,
                left_child: split.left_page,
            });
            let mut left = self.read_page(split.left_page)?;
            left.parent_page = new_root;
            self.write_page(split.left_page, &left)?;
            let mut right = self.read_page(split.right_page)?;
            right.parent_page = new_root;
            self.write_page(split.right_page, &right)?;
            self.write_page(new_root, &root_page)?;
            self.root_page = new_root;
            self.write_superblock()?;
        }
        Ok(())
    }

    /// Return all values for keys in `[start, end]` (inclusive).
    pub fn range_scan(&self, start: i64, end: i64) -> DbResult<Vec<u64>> {
        let mut out = Vec::new();
        self.range_scan_leaf(self.root_page, start, end, &mut out)?;
        Ok(out)
    }
}

struct SplitResult {
    key: i64,
    value: u64,
    left_page: u64,
    right_page: u64,
}

impl<B: PageStorage + StorageBackend> BTree<B> {
    fn insert_recursive(
        &mut self,
        page_num: u64,
        key: i64,
        value: u64,
    ) -> DbResult<Option<SplitResult>> {
        let mut page = self.read_page(page_num)?;
        if page.page_type == LEAF {
            if page.entries.iter().any(|e| e.key() == key) {
                return Err(DbError::syntax(format!("duplicate key {key} in B-Tree")));
            }
            page.entries.push(Entry::Leaf { key, row_id: value });
            page.entries.sort_by_key(|e| e.key());
            if !page.is_full() {
                self.write_page(page_num, &page)?;
                return Ok(None);
            }

            let mid = page.entries.len() / 2;
            let right_num = self.alloc_page()?;
            let mut right = Page::new_leaf();
            right.parent_page = page.parent_page;
            right.entries = page.entries[mid..].to_vec();
            let sep_key = right.entries[0].key();
            let sep_val = right.entries[0].row_id();
            page.entries.truncate(mid);
            debug_assert!(page.entries.len() >= MIN_ENTRIES, "leaf left-page underflow after split");
            debug_assert!(right.entries.len() >= MIN_ENTRIES, "leaf right-page underflow after split");
            self.write_page(page_num, &page)?;
            self.write_page(right_num, &right)?;
            return Ok(Some(SplitResult {
                key: sep_key,
                value: sep_val,
                left_page: page_num,
                right_page: right_num,
            }));
        }

        let child = {
            let mut c = page.rightmost_child;
            for e in &page.entries {
                if key < e.key() {
                    if let Entry::Internal { left_child, .. } = e {
                        c = *left_child;
                        break;
                    }
                }
            }
            c
        };
        let Some(split) = self.insert_recursive(child, key, value)? else {
            return Ok(None);
        };

        let pos = page
            .entries
            .iter()
            .position(|e| split.key < e.key())
            .unwrap_or(page.entries.len());
        page.entries.insert(
            pos,
            Entry::Internal {
                key: split.key,
                row_id: split.value,
                left_child: split.left_page,
            },
        );
        if pos + 1 < page.entries.len() {
            if let Entry::Internal { left_child, .. } = &mut page.entries[pos + 1] {
                *left_child = split.right_page;
            }
        } else {
            // Separator appended at the end → update rightmost_child.
            page.rightmost_child = split.right_page;
        }
        if !page.is_full() {
            self.write_page(page_num, &page)?;
            return Ok(None);
        }

        let mid = page.entries.len() / 2;
        let sep = page.entries[mid].clone();
        let right_num = self.alloc_page()?;
        let mut right = Page::new_internal(page.rightmost_child);
        right.parent_page = page.parent_page;
        right.entries = page.entries[mid + 1..].to_vec();
        let lr = if let Entry::Internal { left_child, .. } = &sep {
            *left_child
        } else {
            0
        };
        page.rightmost_child = lr;
        page.entries.truncate(mid);
        debug_assert!(page.entries.len() >= MIN_ENTRIES, "internal left-page underflow after split");
        debug_assert!(right.entries.len() >= MIN_ENTRIES, "internal right-page underflow after split");
        for e in &right.entries {
            if let Entry::Internal { left_child, .. } = e {
                let mut cp = self.read_page(*left_child)?;
                cp.parent_page = right_num;
                self.write_page(*left_child, &cp)?;
            }
        }
        {
            let mut rc = self.read_page(right.rightmost_child)?;
            rc.parent_page = right_num;
            self.write_page(right.rightmost_child, &rc)?;
        }
        self.write_page(page_num, &page)?;
        self.write_page(right_num, &right)?;
        Ok(Some(SplitResult {
            key: sep.key(),
            value: sep.row_id(),
            left_page: page_num,
            right_page: right_num,
        }))
    }

    fn range_scan_leaf(
        &self,
        page_num: u64,
        start: i64,
        end: i64,
        out: &mut Vec<u64>,
    ) -> DbResult<()> {
        let page = self.read_page(page_num)?;
        if page.page_type == LEAF {
            for e in &page.entries {
                let k = e.key();
                if k >= start && k <= end {
                    out.push(e.row_id());
                }
                if k > end {
                    break;
                }
            }
            if page.parent_page > 0 {
                self.walk_next_leaf(page.parent_page, page_num, start, end, out)?;
            }
            return Ok(());
        }
        for e in &page.entries {
            if start < e.key() {
                if let Entry::Internal { left_child, .. } = e {
                    return self.range_scan_leaf(*left_child, start, end, out);
                }
            }
        }
        self.range_scan_leaf(page.rightmost_child, start, end, out)
    }

    fn walk_next_leaf(
        &self,
        parent: u64,
        from: u64,
        start: i64,
        end: i64,
        out: &mut Vec<u64>,
    ) -> DbResult<()> {
        let p = self.read_page(parent)?;
        let mut next = None;
        for (i, e) in p.entries.iter().enumerate() {
            if let Entry::Internal { left_child, .. } = e {
                if *left_child == from {
                    next = if i + 1 < p.entries.len() {
                        if let Entry::Internal { left_child, .. } = &p.entries[i + 1] {
                            Some(*left_child)
                        } else {
                            None
                        }
                    } else {
                        Some(p.rightmost_child)
                    };
                    break;
                }
            }
        }
        if let Some(n) = next {
            let leaf = self.read_page(n)?;
            for e in &leaf.entries {
                let k = e.key();
                if k >= start && k <= end {
                    out.push(e.row_id());
                }
                if k > end {
                    return Ok(());
                }
            }
            if leaf.parent_page > 0 {
                self.walk_next_leaf(leaf.parent_page, n, start, end, out)?;
            }
        }
        Ok(())
    }
}

// ═══════════════════════════════════════════════════════════════════════
//  BTree Backend — StorageBackend implementation
// ═══════════════════════════════════════════════════════════════════════

/// Backend that stores table rows in a B-Tree index + companion data file.
///
/// Per-table layout (using `<table>.btree` as base path):
///   `<table>.idx` — B-Tree: key=row_id (i64), value=offset in .dat
///   `<table>.dat` — length-prefixed JSON rows
///
/// The internal page-level I/O for the B-tree is delegated to [JsonBackend].
#[derive(Debug, Clone, Default)]
pub struct BTreeBackend;

impl BTreeBackend {
    /// Returns `true` if the path is a BTree table path (`.btree` extension).
    fn is_table_path(path: &Path) -> bool {
        path.extension().map(|e| e == "btree").unwrap_or(false)
    }

    fn idx_path(table_path: &Path) -> PathBuf {
        table_path.with_extension("idx")
    }

    fn dat_path(table_path: &Path) -> PathBuf {
        table_path.with_extension("dat")
    }

    /// Open the B-tree index for a table.
    fn open_index(&self, table_path: &Path) -> DbResult<BTree<JsonBackend>> {
        let idx = Self::idx_path(table_path);
        if JsonBackend.file_exists(&idx) {
            BTree::open(JsonBackend, &idx)
        } else {
            BTree::create(JsonBackend, &idx)
        }
    }

    /// Read a StoredRow from the data file at the given offset.
    fn read_row_at(dat_path: &Path, offset: u64) -> DbResult<StoredRow> {
        let mut file = File::open(dat_path)?;
        file.seek(SeekFrom::Start(offset))?;
        let mut len_buf = [0u8; 4];
        file.read_exact(&mut len_buf)?;
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut json_buf = vec![0u8; len];
        file.read_exact(&mut json_buf)?;
        Ok(serde_json::from_slice(&json_buf)?)
    }

    /// Append a StoredRow to the data file, return its offset.
    fn append_row_to_dat(dat_path: &Path, row: &StoredRow) -> DbResult<u64> {
        let json = serde_json::to_vec(row)?;
        let len = json.len() as u32;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(dat_path)?;
        let offset = file.seek(SeekFrom::End(0))?;
        file.write_all(&len.to_le_bytes())?;
        file.write_all(&json)?;
        Ok(offset)
    }
}

impl StorageBackend for BTreeBackend {
    fn schema_path(&self, root: &Path) -> PathBuf {
        root.join("schema.json")
    }
    fn wal_path(&self, root: &Path) -> PathBuf {
        root.join("wal.log")
    }
    fn table_path(&self, root: &Path, table: &str) -> PathBuf {
        root.join(format!("{table}.btree"))
    }

    fn load_schema(&self, path: &Path) -> DbResult<DatabaseSchema> {
        JsonBackend.load_schema(path)
    }

    fn save_schema(&self, path: &Path, schema: &DatabaseSchema) -> DbResult<()> {
        JsonBackend.save_schema(path, schema)
    }

    fn scan_rows<F>(&self, table_path: &Path, mut func: F) -> DbResult<()>
    where
        F: FnMut(&StoredRow) -> DbResult<()>,
    {
        let idx = self.open_index(table_path)?;
        let dat = Self::dat_path(table_path);
        if !JsonBackend.file_exists(&dat) {
            return Ok(());
        }

        let offsets = idx.range_scan(i64::MIN, i64::MAX)?;
        for offset in offsets {
            let row = Self::read_row_at(&dat, offset)?;
            func(&row)?;
        }
        Ok(())
    }

    fn append_row(&self, table_path: &Path, row: &StoredRow) -> DbResult<()> {
        let offset = Self::append_row_to_dat(&Self::dat_path(table_path), row)?;
        let mut idx = self.open_index(table_path)?;
        idx.insert(row.row_id as i64, offset)
    }

    fn rewrite_rows(&self, table_path: &Path, rows: &[StoredRow]) -> DbResult<()> {
        let idx_path = Self::idx_path(table_path);
        let dat_path = Self::dat_path(table_path);

        // Build new index + data in temporary files so a crash during
        // the rewrite leaves the originals intact.  fs::rename is atomic
        // on Linux and silently replaces the target.
        let tmp_idx = idx_path.with_extension("idx.tmp");
        let tmp_dat = dat_path.with_extension("dat.tmp");

        let mut idx = BTree::create(JsonBackend, &tmp_idx)?;
        // Always create the data file, even for empty tables.
        JsonBackend.create_file(&tmp_dat)?;
        for row in rows {
            let offset = Self::append_row_to_dat(&tmp_dat, row)?;
            idx.insert(row.row_id as i64, offset)?;
        }

        // Atomically swap temps over originals.
        JsonBackend.rename_file(&tmp_idx, &idx_path)?;
        JsonBackend.rename_file(&tmp_dat, &dat_path)?;
        Ok(())
    }

    fn create_file(&self, path: &Path) -> DbResult<()> {
        let idx_path = Self::idx_path(path);
        BTree::create(JsonBackend, &idx_path)?;
        File::create(Self::dat_path(path))?;
        Ok(())
    }

    fn remove_file(&self, path: &Path) -> DbResult<()> {
        if Self::is_table_path(path) {
            let idx = Self::idx_path(path);
            let dat = Self::dat_path(path);
            if JsonBackend.file_exists(&idx) {
                JsonBackend.remove_file(&idx)?;
            }
            if JsonBackend.file_exists(&dat) {
                JsonBackend.remove_file(&dat)?;
            }
        } else {
            JsonBackend.remove_file(path)?;
        }
        Ok(())
    }

    fn rename_file(&self, from: &Path, to: &Path) -> DbResult<()> {
        if Self::is_table_path(from) && Self::is_table_path(to) {
            JsonBackend.rename_file(&Self::idx_path(from), &Self::idx_path(to))?;
            JsonBackend.rename_file(&Self::dat_path(from), &Self::dat_path(to))
        } else {
            JsonBackend.rename_file(from, to)
        }
    }

    fn file_exists(&self, path: &Path) -> bool {
        if Self::is_table_path(path) {
            JsonBackend.file_exists(&Self::idx_path(path))
        } else {
            JsonBackend.file_exists(path)
        }
    }

    fn create_dir_all(&self, path: &Path) -> DbResult<()> {
        JsonBackend.create_dir_all(path)
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod integration_tests;
