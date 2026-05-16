//! Manual binary serialization for `WalRecord`.
//!
//! Used by the btree feature when `serde` is not available.

use std::io::{Read, Write};

use crate::error::{DbError, DbResult};
use crate::schema::{DatabaseSchema, Value};
use crate::storage::StoredRow;

use super::WalRecord;

// ── Write helpers ──────────────────────────────────────────────────────

fn w_u16(w: &mut impl Write, v: u16) -> std::io::Result<()> {
    w.write_all(&v.to_le_bytes())
}
fn w_u32(w: &mut impl Write, v: u32) -> std::io::Result<()> {
    w.write_all(&v.to_le_bytes())
}
fn w_u64(w: &mut impl Write, v: u64) -> std::io::Result<()> {
    w.write_all(&v.to_le_bytes())
}
fn w_u8(w: &mut impl Write, v: u8) -> std::io::Result<()> {
    w.write_all(&[v])
}
fn w_str(w: &mut impl Write, s: &str) -> std::io::Result<()> {
    let b = s.as_bytes();
    w_u16(w, b.len() as u16)?;
    w.write_all(b)
}
fn w_values(w: &mut impl Write, vals: &[Value]) -> std::io::Result<()> {
    w_u16(w, vals.len() as u16)?;
    for v in vals {
        match v {
            Value::Null => w_u8(w, 0x00)?,
            Value::Int(i) => { w_u8(w, 0x01)?; w_u64(w, *i as u64)?; }
            Value::Float(f) => { w_u8(w, 0x02)?; w.write_all(&f.to_le_bytes())?; }
            Value::Str(s) => { w_u8(w, 0x03)?; w_str(w, s)?; }
        }
    }
    Ok(())
}
fn w_stored_rows(w: &mut impl Write, rows: &[StoredRow]) -> std::io::Result<()> {
    w_u32(w, rows.len() as u32)?;
    for r in rows {
        w_u64(w, r.row_id)?;
        w_values(w, &r.values)?;
    }
    Ok(())
}

// ── Read helpers ───────────────────────────────────────────────────────

fn r_u16(r: &mut impl Read) -> std::io::Result<u16> {
    let mut b = [0u8; 2]; r.read_exact(&mut b)?; Ok(u16::from_le_bytes(b))
}
fn r_u32(r: &mut impl Read) -> std::io::Result<u32> {
    let mut b = [0u8; 4]; r.read_exact(&mut b)?; Ok(u32::from_le_bytes(b))
}
fn r_u64(r: &mut impl Read) -> std::io::Result<u64> {
    let mut b = [0u8; 8]; r.read_exact(&mut b)?; Ok(u64::from_le_bytes(b))
}
fn r_u8(r: &mut impl Read) -> std::io::Result<u8> {
    let mut b = [0u8; 1]; r.read_exact(&mut b)?; Ok(b[0])
}
fn r_str(r: &mut impl Read) -> std::io::Result<String> {
    let len = r_u16(r)? as usize;
    let mut b = vec![0u8; len];
    r.read_exact(&mut b)?;
    String::from_utf8(b).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}
fn r_values(r: &mut impl Read) -> std::io::Result<Vec<Value>> {
    let n = r_u16(r)? as usize;
    let mut vals = Vec::with_capacity(n);
    for _ in 0..n {
        let tag = r_u8(r)?;
        vals.push(match tag {
            0x00 => Value::Null,
            0x01 => Value::Int(r_u64(r)? as i64),
            0x02 => {
                let mut b = [0u8; 8]; r.read_exact(&mut b)?;
                Value::Float(f64::from_le_bytes(b))
            }
            0x03 => Value::Str(r_str(r)?),
            _ => return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, format!("bad value tag {tag}"))),
        });
    }
    Ok(vals)
}
fn r_stored_rows(r: &mut impl Read) -> std::io::Result<Vec<StoredRow>> {
    let n = r_u32(r)? as usize;
    let mut rows = Vec::with_capacity(n);
    for _ in 0..n {
        let row_id = r_u64(r)?;
        let values = r_values(r)?;
        rows.push(StoredRow { row_id, values });
    }
    Ok(rows)
}

// ── Public API ─────────────────────────────────────────────────────────

/// Serialize a `WalRecord` into a binary buffer.
pub fn serialize_wal(record: &WalRecord) -> std::io::Result<Vec<u8>> {
    let mut buf = Vec::new();
    match record {
        WalRecord::InsertRow { table, row_id } => {
            w_u8(&mut buf, 0)?;
            w_str(&mut buf, table)?;
            w_u64(&mut buf, *row_id)?;
        }
        WalRecord::UpdateRow { table, row_id, old_values } => {
            w_u8(&mut buf, 1)?;
            w_str(&mut buf, table)?;
            w_u64(&mut buf, *row_id)?;
            w_values(&mut buf, old_values)?;
        }
        WalRecord::DeleteRow { table, row_id, old_values } => {
            w_u8(&mut buf, 2)?;
            w_str(&mut buf, table)?;
            w_u64(&mut buf, *row_id)?;
            w_values(&mut buf, old_values)?;
        }
        WalRecord::RewriteTable { table, old_rows } => {
            w_u8(&mut buf, 3)?;
            w_str(&mut buf, table)?;
            w_stored_rows(&mut buf, old_rows)?;
        }
        WalRecord::ReplaceSchema { old_schema } => {
            w_u8(&mut buf, 4)?;
            use crate::storage::schema_binary;
            let data = schema_binary::serialize_schema(old_schema)?;
            w_u32(&mut buf, data.len() as u32)?;
            buf.write_all(&data)?;
        }
        WalRecord::DropTableFile { table } => {
            w_u8(&mut buf, 5)?;
            w_str(&mut buf, table)?;
        }
        WalRecord::RestoreTableFile { table, rows } => {
            w_u8(&mut buf, 6)?;
            w_str(&mut buf, table)?;
            w_stored_rows(&mut buf, rows)?;
        }
        WalRecord::RenameTable { old_name, new_name } => {
            w_u8(&mut buf, 7)?;
            w_str(&mut buf, old_name)?;
            w_str(&mut buf, new_name)?;
        }
    }
    Ok(buf)
}

/// Deserialize a `WalRecord` from a binary buffer.
pub fn deserialize_wal(data: &[u8]) -> DbResult<WalRecord> {
    let mut r = std::io::Cursor::new(data);
    let tag = r_u8(&mut r)?;
    Ok(match tag {
        0 => WalRecord::InsertRow { table: r_str(&mut r)?, row_id: r_u64(&mut r)? },
        1 => WalRecord::UpdateRow { table: r_str(&mut r)?, row_id: r_u64(&mut r)?, old_values: r_values(&mut r)? },
        2 => WalRecord::DeleteRow { table: r_str(&mut r)?, row_id: r_u64(&mut r)?, old_values: r_values(&mut r)? },
        3 => WalRecord::RewriteTable { table: r_str(&mut r)?, old_rows: r_stored_rows(&mut r)? },
        4 => {
            let len = r_u32(&mut r)? as usize;
            let mut schema_data = vec![0u8; len];
            r.read_exact(&mut schema_data)?;
            use crate::storage::schema_binary;
            let old_schema = schema_binary::deserialize_schema(&schema_data)?;
            WalRecord::ReplaceSchema { old_schema }
        }
        5 => WalRecord::DropTableFile { table: r_str(&mut r)? },
        6 => WalRecord::RestoreTableFile { table: r_str(&mut r)?, rows: r_stored_rows(&mut r)? },
        7 => WalRecord::RenameTable { old_name: r_str(&mut r)?, new_name: r_str(&mut r)? },
        _ => return Err(DbError::StorageCorruption(format!("unknown WAL record tag: {tag}"))),
    })
}
