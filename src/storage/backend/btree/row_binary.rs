//! Binary row serialization for the `.dat` file.
//!
//! On-disk format (see `layout.md` for the full specification):
//! ```text
//! [row_len: u32 LE][row_data...]
//! row_data:
//!   [row_id: u64 LE][value_count: u16 LE][tag+payload...]
//! tag: 0x00=Null  0x01=Int(i64LE)  0x02=Float(f64LE)  0x03=Str(u16LE+utf8)
//! ```

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::error::DbResult;
use crate::schema::Value;

use super::StoredRow;

// ── Value tags ─────────────────────────────────────────────────────────

const TAG_NULL: u8 = 0x00;
const TAG_INT: u8 = 0x01;
const TAG_FLOAT: u8 = 0x02;
const TAG_STR: u8 = 0x03;

// ── Serialization ──────────────────────────────────────────────────────

/// Serialize a `StoredRow` into the binary `.dat` format.
pub fn serialize_row(row: &StoredRow) -> Vec<u8> {
    let mut buf = Vec::new();

    // row_id (u64 LE)
    buf.extend_from_slice(&row.row_id.to_le_bytes());

    // value_count (u16 LE)
    let n = row.values.len() as u16;
    buf.extend_from_slice(&n.to_le_bytes());

    // values
    for v in &row.values {
        match v {
            Value::Null => {
                buf.push(TAG_NULL);
            }
            Value::Int(i) => {
                buf.push(TAG_INT);
                buf.extend_from_slice(&i.to_le_bytes());
            }
            Value::Float(f) => {
                buf.push(TAG_FLOAT);
                buf.extend_from_slice(&f.to_le_bytes());
            }
            Value::Str(s) => {
                let bytes = s.as_bytes();
                let len = bytes.len() as u16;
                buf.push(TAG_STR);
                buf.extend_from_slice(&len.to_le_bytes());
                buf.extend_from_slice(bytes);
            }
        }
    }

    buf
}

/// Deserialize a `StoredRow` from binary `.dat` format bytes.
pub fn deserialize_row(data: &[u8]) -> DbResult<StoredRow> {
    if data.len() < 10 {
        return Err(crate::error::DbError::StorageCorruption(
            "row data too short".into(),
        ));
    }

    let row_id = u64::from_le_bytes(data[0..8].try_into().unwrap());
    let value_count = u16::from_le_bytes(data[8..10].try_into().unwrap()) as usize;

    let mut values = Vec::with_capacity(value_count);
    let mut pos: usize = 10;

    for _ in 0..value_count {
        if pos >= data.len() {
            return Err(crate::error::DbError::StorageCorruption(
                "row data truncated at value tag".into(),
            ));
        }
        let tag = data[pos];
        pos += 1;

        match tag {
            TAG_NULL => {
                values.push(Value::Null);
            }
            TAG_INT => {
                if pos + 8 > data.len() {
                    return Err(crate::error::DbError::StorageCorruption(
                        "row data truncated in Int value".into(),
                    ));
                }
                let i = i64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                values.push(Value::Int(i));
                pos += 8;
            }
            TAG_FLOAT => {
                if pos + 8 > data.len() {
                    return Err(crate::error::DbError::StorageCorruption(
                        "row data truncated in Float value".into(),
                    ));
                }
                let f = f64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                values.push(Value::Float(f));
                pos += 8;
            }
            TAG_STR => {
                if pos + 2 > data.len() {
                    return Err(crate::error::DbError::StorageCorruption(
                        "row data truncated in Str length".into(),
                    ));
                }
                let len = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
                pos += 2;
                if pos + len > data.len() {
                    return Err(crate::error::DbError::StorageCorruption(
                        "row data truncated in Str bytes".into(),
                    ));
                }
                let s = String::from_utf8(data[pos..pos + len].to_vec()).map_err(|_| {
                    crate::error::DbError::StorageCorruption("invalid UTF-8 in Str value".into())
                })?;
                values.push(Value::Str(s));
                pos += len;
            }
            _ => {
                return Err(crate::error::DbError::StorageCorruption(format!(
                    "unknown value tag: 0x{tag:02x}"
                )));
            }
        }
    }

    Ok(StoredRow { row_id, values })
}

// ── File-level I/O ─────────────────────────────────────────────────────

/// Read a single `StoredRow` from the data file at the given byte offset.
pub fn read_row_at(dat_path: &Path, offset: u64) -> DbResult<StoredRow> {
    let mut file = File::open(dat_path)?;
    file.seek(SeekFrom::Start(offset))?;

    // Read length prefix (u32 LE)
    let mut len_buf = [0u8; 4];
    file.read_exact(&mut len_buf)?;
    let row_len = u32::from_le_bytes(len_buf) as usize;

    // Read row data
    let mut row_buf = vec![0u8; row_len];
    file.read_exact(&mut row_buf)?;

    deserialize_row(&row_buf)
}

/// Append a `StoredRow` to the data file.  Returns the byte offset where
/// the row was written.
pub fn append_row_to_dat(dat_path: &Path, row: &StoredRow) -> DbResult<u64> {
    let row_data = serialize_row(row);
    let row_len = row_data.len() as u32;

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(dat_path)?;
    let offset = file.seek(SeekFrom::End(0))?;

    file.write_all(&row_len.to_le_bytes())?;
    file.write_all(&row_data)?;
    file.flush()?;

    Ok(offset)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn test_row(row_id: u64, values: Vec<Value>) -> StoredRow {
        StoredRow { row_id, values }
    }

    #[test]
    fn roundtrip_int_null_str() {
        let row = test_row(
            7,
            vec![Value::Int(42), Value::Null, Value::Str("hi".into())],
        );
        let data = serialize_row(&row);
        assert_eq!(data.len(), 25, "expected 25 bytes per layout spec");
        let row2 = deserialize_row(&data).unwrap();
        assert_eq!(row, row2);
    }

    #[test]
    fn roundtrip_float() {
        let row = test_row(1, vec![Value::Float(3.14)]);
        let row2 = deserialize_row(&serialize_row(&row)).unwrap();
        assert_eq!(row, row2);
    }

    #[test]
    fn roundtrip_null_only() {
        let row = test_row(0, vec![Value::Null, Value::Null]);
        let row2 = deserialize_row(&serialize_row(&row)).unwrap();
        assert_eq!(row, row2);
    }

    #[test]
    fn roundtrip_empty_values() {
        let row = test_row(99, vec![]);
        let row2 = deserialize_row(&serialize_row(&row)).unwrap();
        assert_eq!(row, row2);
    }

    #[test]
    fn file_roundtrip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.dat");

        let row = test_row(5, vec![Value::Int(-1), Value::Str("x".into())]);
        let offset = append_row_to_dat(&path, &row).unwrap();
        assert_eq!(offset, 0); // first write to empty file

        let row2 = read_row_at(&path, offset).unwrap();
        assert_eq!(row, row2);
    }

    #[test]
    fn multiple_rows_in_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.dat");

        let r1 = test_row(1, vec![Value::Int(10)]);
        let r2 = test_row(2, vec![Value::Str("hello".into())]);

        let off1 = append_row_to_dat(&path, &r1).unwrap();
        let off2 = append_row_to_dat(&path, &r2).unwrap();

        assert_ne!(off1, off2);
        assert_eq!(read_row_at(&path, off1).unwrap(), r1);
        assert_eq!(read_row_at(&path, off2).unwrap(), r2);
    }
}
