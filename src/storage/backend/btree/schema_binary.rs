//! Manual binary serialization for `DatabaseSchema`.
//!
//! Used by the btree backend when `serde` / `serde_json` are not available.
//! The format is self-describing with length-prefixed strings.

use std::io::{Read, Write};

use crate::error::DbResult;
use crate::schema::{ColumnSchema, ColumnType, DatabaseSchema, IndexSchema, TableSchema, Value};

// ── Write helpers ──────────────────────────────────────────────────────

fn write_u16(w: &mut impl Write, v: u16) -> std::io::Result<()> {
    w.write_all(&v.to_le_bytes())
}
fn write_u8(w: &mut impl Write, v: u8) -> std::io::Result<()> {
    w.write_all(&[v])
}
fn write_str(w: &mut impl Write, s: &str) -> std::io::Result<()> {
    let bytes = s.as_bytes();
    write_u16(w, bytes.len() as u16)?;
    w.write_all(bytes)
}
fn write_value(w: &mut impl Write, v: &Value) -> std::io::Result<()> {
    match v {
        Value::Null => write_u8(w, 0x00)?,
        Value::Int(i) => {
            write_u8(w, 0x01)?;
            w.write_all(&i.to_le_bytes())?;
        }
        Value::Float(f) => {
            write_u8(w, 0x02)?;
            w.write_all(&f.to_le_bytes())?;
        }
        Value::Str(s) => {
            write_u8(w, 0x03)?;
            write_str(w, s)?;
        }
    }
    Ok(())
}

// ── Read helpers ───────────────────────────────────────────────────────

fn read_u16(r: &mut impl Read) -> std::io::Result<u16> {
    let mut buf = [0u8; 2];
    r.read_exact(&mut buf)?;
    Ok(u16::from_le_bytes(buf))
}
fn read_u8(r: &mut impl Read) -> std::io::Result<u8> {
    let mut buf = [0u8; 1];
    r.read_exact(&mut buf)?;
    Ok(buf[0])
}
fn read_str(r: &mut impl Read) -> std::io::Result<String> {
    let len = read_u16(r)? as usize;
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    String::from_utf8(buf).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}
fn read_value(r: &mut impl Read) -> std::io::Result<Value> {
    let tag = read_u8(r)?;
    match tag {
        0x00 => Ok(Value::Null),
        0x01 => {
            let mut buf = [0u8; 8];
            r.read_exact(&mut buf)?;
            Ok(Value::Int(i64::from_le_bytes(buf)))
        }
        0x02 => {
            let mut buf = [0u8; 8];
            r.read_exact(&mut buf)?;
            Ok(Value::Float(f64::from_le_bytes(buf)))
        }
        0x03 => {
            let s = read_str(r)?;
            Ok(Value::Str(s))
        }
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("unknown value tag: 0x{tag:02x}"),
        )),
    }
}

// ── Schema serialization ───────────────────────────────────────────────

/// Serialize a `DatabaseSchema` into the binary schema format.
pub fn serialize_schema(schema: &DatabaseSchema) -> std::io::Result<Vec<u8>> {
    let mut buf = Vec::new();

    write_u16(&mut buf, 0x5343)?; // magic "SC"
    write_str(&mut buf, &schema.name)?;
    write_u16(&mut buf, schema.tables.len() as u16)?;

    for (_, table) in &schema.tables {
        write_str(&mut buf, &table.name)?;
        write_u16(&mut buf, table.columns.len() as u16)?;
        for col in &table.columns {
            write_str(&mut buf, &col.name)?;
            match &col.col_type {
                ColumnType::Int => write_u8(&mut buf, 0)?,
                ColumnType::Float => write_u8(&mut buf, 1)?,
                ColumnType::Varchar(len) => {
                    write_u8(&mut buf, 2)?;
                    write_u16(&mut buf, *len as u16)?;
                }
            }
            write_u8(&mut buf, if col.not_null { 1 } else { 0 })?;
            write_u8(&mut buf, if col.unique { 1 } else { 0 })?;
            if let Some(ref default) = col.default {
                write_u8(&mut buf, 1)?;
                write_value(&mut buf, default)?;
            } else {
                write_u8(&mut buf, 0)?;
            }
        }
    }

    write_u16(&mut buf, schema.indexes.len() as u16)?;
    for (_, idx) in &schema.indexes {
        write_str(&mut buf, &idx.name)?;
        write_str(&mut buf, &idx.table_name)?;
        write_str(&mut buf, &idx.column)?;
    }

    Ok(buf)
}

/// Deserialize a `DatabaseSchema` from the binary schema format.
pub fn deserialize_schema(data: &[u8]) -> DbResult<DatabaseSchema> {
    let mut r = std::io::Cursor::new(data);

    let magic = read_u16(&mut r)?;
    if magic != 0x5343 {
        return Err(crate::error::DbError::StorageCorruption(format!(
            "bad schema magic: 0x{magic:04x}"
        )));
    }

    let name = read_str(&mut r)?;
    let table_count = read_u16(&mut r)? as usize;
    let mut tables = std::collections::HashMap::new();

    for _ in 0..table_count {
        let tname = read_str(&mut r)?;
        let col_count = read_u16(&mut r)? as usize;
        let mut columns = Vec::with_capacity(col_count);

        for _ in 0..col_count {
            let cname = read_str(&mut r)?;
            let col_type = match read_u8(&mut r)? {
                0 => ColumnType::Int,
                1 => ColumnType::Float,
                2 => {
                    let len = read_u16(&mut r)? as usize;
                    ColumnType::Varchar(len)
                }
                t => {
                    return Err(crate::error::DbError::StorageCorruption(format!(
                        "unknown column type tag: {t}"
                    )))
                }
            };
            let not_null = read_u8(&mut r)? != 0;
            let unique = read_u8(&mut r)? != 0;
            let has_default = read_u8(&mut r)? != 0;
            let default = if has_default {
                Some(read_value(&mut r)?)
            } else {
                None
            };

            columns.push(ColumnSchema {
                name: cname,
                col_type,
                not_null,
                unique,
                default,
            });
        }

        tables.insert(tname.clone(), TableSchema { name: tname, columns });
    }

    let index_count = read_u16(&mut r)? as usize;
    let mut indexes = std::collections::HashMap::new();

    for _ in 0..index_count {
        let iname = read_str(&mut r)?;
        let itable = read_str(&mut r)?;
        let icol = read_str(&mut r)?;
        indexes.insert(
            iname.clone(),
            IndexSchema {
                name: iname,
                table_name: itable,
                column: icol,
            },
        );
    }

    Ok(DatabaseSchema {
        name,
        tables,
        indexes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ColumnSchema;

    fn sample_schema() -> DatabaseSchema {
        let mut schema = DatabaseSchema::new("testdb");
        schema
            .add_table(TableSchema::new(
                "users",
                vec![
                    ColumnSchema::with_nullability("id", ColumnType::Int, true),
                    ColumnSchema::new("name", ColumnType::Varchar(10)),
                ],
            ))
            .unwrap();
        schema
    }

    #[test]
    fn roundtrip_empty() {
        let s = DatabaseSchema::new("empty");
        let data = serialize_schema(&s).unwrap();
        let s2 = deserialize_schema(&data).unwrap();
        assert_eq!(s2.name, "empty");
        assert!(s2.tables.is_empty());
    }

    #[test]
    fn roundtrip_basic() {
        let s = sample_schema();
        let data = serialize_schema(&s).unwrap();
        let s2 = deserialize_schema(&data).unwrap();
        assert_eq!(s2.name, "testdb");
        assert_eq!(s2.tables.len(), 1);
        let t = s2.get_table("users").unwrap();
        assert_eq!(t.columns.len(), 2);
        assert_eq!(t.columns[0].name, "id");
        assert!(t.columns[0].not_null);
        assert_eq!(t.columns[1].name, "name");
    }
}
