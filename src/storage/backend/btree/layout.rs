//! B-Tree page layout and serialization.
//!
//! Every page is exactly 4096 bytes. The first 24 bytes are a fixed header,
//! followed by a sorted array of fixed-size entries.

use crate::error::{DbError, DbResult};

// ── Constants ─────────────────────────────────────────────────────────

pub const PAGE_SIZE: usize = 4096;
pub const HEADER_SIZE: usize = 24;

pub const LEAF: u8 = 0;
pub const INTERNAL: u8 = 1;

pub const LEAF_ENTRY_SIZE: usize = 16;
pub const INTERNAL_ENTRY_SIZE: usize = 24;

pub const MAX_LEAF_ENTRIES: usize = (PAGE_SIZE - HEADER_SIZE) / LEAF_ENTRY_SIZE; // 253
pub const MAX_INTERNAL_ENTRIES: usize = (PAGE_SIZE - HEADER_SIZE) / INTERNAL_ENTRY_SIZE; // 169
pub const MIN_ENTRIES: usize = 2;

// ── Entry ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum Entry {
    Leaf {
        key: i64,
        row_id: u64,
    },
    Internal {
        key: i64,
        row_id: u64,
        left_child: u64,
    },
}

impl Entry {
    pub fn key(&self) -> i64 {
        match self {
            Entry::Leaf { key, .. } | Entry::Internal { key, .. } => *key,
        }
    }
    pub fn row_id(&self) -> u64 {
        match self {
            Entry::Leaf { row_id, .. } | Entry::Internal { row_id, .. } => *row_id,
        }
    }
}

// ── Page ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct Page {
    pub page_type: u8,
    pub parent_page: u64,
    pub rightmost_child: u64,
    pub entries: Vec<Entry>,
}

impl Page {
    pub fn new_leaf() -> Self {
        Page {
            page_type: LEAF,
            parent_page: 0,
            rightmost_child: 0,
            entries: Vec::new(),
        }
    }

    pub fn new_internal(rightmost_child: u64) -> Self {
        Page {
            page_type: INTERNAL,
            parent_page: 0,
            rightmost_child,
            entries: Vec::new(),
        }
    }

    pub fn capacity(&self) -> usize {
        if self.page_type == LEAF {
            MAX_LEAF_ENTRIES
        } else {
            MAX_INTERNAL_ENTRIES
        }
    }

    pub fn is_full(&self) -> bool {
        self.entries.len() >= self.capacity()
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = vec![0u8; PAGE_SIZE];
        buf[0] = self.page_type;
        let n = self.entries.len() as u16;
        buf[1..3].copy_from_slice(&n.to_le_bytes());
        buf[3..11].copy_from_slice(&self.parent_page.to_le_bytes());
        buf[11..19].copy_from_slice(&self.rightmost_child.to_le_bytes());

        let mut off = HEADER_SIZE;
        for e in &self.entries {
            match e {
                Entry::Leaf { key, row_id } => {
                    buf[off..off + 8].copy_from_slice(&key.to_le_bytes());
                    buf[off + 8..off + 16].copy_from_slice(&row_id.to_le_bytes());
                    off += LEAF_ENTRY_SIZE;
                }
                Entry::Internal {
                    key,
                    row_id,
                    left_child,
                } => {
                    buf[off..off + 8].copy_from_slice(&key.to_le_bytes());
                    buf[off + 8..off + 16].copy_from_slice(&row_id.to_le_bytes());
                    buf[off + 16..off + 24].copy_from_slice(&left_child.to_le_bytes());
                    off += INTERNAL_ENTRY_SIZE;
                }
            }
        }
        buf
    }

    pub fn deserialize(data: &[u8]) -> DbResult<Self> {
        if data.len() < PAGE_SIZE {
            return Err(DbError::syntax("page buffer too short"));
        }
        let page_type = data[0];
        if page_type != LEAF && page_type != INTERNAL {
            return Err(DbError::syntax(format!("invalid page type: {page_type}")));
        }
        let num = u16::from_le_bytes([data[1], data[2]]) as usize;
        let parent_page = u64::from_le_bytes(data[3..11].try_into().unwrap());
        let rightmost_child = u64::from_le_bytes(data[11..19].try_into().unwrap());

        let entry_size = if page_type == LEAF {
            LEAF_ENTRY_SIZE
        } else {
            INTERNAL_ENTRY_SIZE
        };
        let max = if page_type == LEAF {
            MAX_LEAF_ENTRIES
        } else {
            MAX_INTERNAL_ENTRIES
        };
        if num > max {
            return Err(DbError::syntax(format!(
                "page has {num} entries, max {max}"
            )));
        }

        let mut entries = Vec::with_capacity(num);
        let mut off = HEADER_SIZE;
        for _ in 0..num {
            let key = i64::from_le_bytes(data[off..off + 8].try_into().unwrap());
            let row_id = u64::from_le_bytes(data[off + 8..off + 16].try_into().unwrap());
            if page_type == LEAF {
                entries.push(Entry::Leaf { key, row_id });
            } else {
                let left_child = u64::from_le_bytes(data[off + 16..off + 24].try_into().unwrap());
                entries.push(Entry::Internal {
                    key,
                    row_id,
                    left_child,
                });
            }
            off += entry_size;
        }
        Ok(Page {
            page_type,
            parent_page,
            rightmost_child,
            entries,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_empty_leaf() {
        let p = Page::new_leaf();
        let p2 = Page::deserialize(&p.serialize()).unwrap();
        assert_eq!(p2.page_type, LEAF);
        assert!(p2.entries.is_empty());
    }

    #[test]
    fn roundtrip_leaf_with_data() {
        let mut p = Page::new_leaf();
        p.entries.push(Entry::Leaf { key: 42, row_id: 7 });
        p.entries.push(Entry::Leaf {
            key: -1,
            row_id: 99,
        });
        let p2 = Page::deserialize(&p.serialize()).unwrap();
        assert_eq!(p2.entries, p.entries);
    }

    #[test]
    fn roundtrip_internal() {
        let mut p = Page::new_internal(5);
        p.entries.push(Entry::Internal {
            key: 100,
            row_id: 1,
            left_child: 2,
        });
        let p2 = Page::deserialize(&p.serialize()).unwrap();
        assert_eq!(p2.page_type, INTERNAL);
        assert_eq!(p2.rightmost_child, 5);
        assert_eq!(p2.entries, p.entries);
    }
}
