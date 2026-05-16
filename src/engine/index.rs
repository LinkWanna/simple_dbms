use pesqlite::{CreateIndex, DropIndex, SchemaObject};

use crate::error::{DbError, DbResult};
use crate::schema::{IndexSchema, Value};

use super::{Engine, ExecutionResult};

/// Hash a cell value into an `i64` key suitable for B-Tree indexing.
///
/// Collisions are possible for STRING values (via djb2).  The index
/// lookup returns a `row_id`; the caller must still verify the column
/// value matches by reading the actual row.
fn value_to_key(v: &Value) -> i64 {
    match v {
        Value::Int(i) => *i,
        Value::Float(f) => f64::to_bits(*f) as i64,
        Value::Str(s) => {
            let mut h: u64 = 5381;
            for b in s.bytes() {
                h = h.wrapping_mul(33).wrapping_add(b as u64);
            }
            h as i64
        }
        Value::Null => 0,
    }
}

impl Engine {
    // ── CREATE INDEX ─────────────────────────────────────────────────

    pub(super) fn execute_create_index(
        &mut self,
        create_index: CreateIndex,
    ) -> DbResult<ExecutionResult> {
        let CreateIndex {
            if_not_exists,
            schema_index: SchemaObject {
                name: index_name, ..
            },
            table_name,
            indexed_cols,
            unique: _unique,
            ..
        } = create_index;

        // Only single-column indexes for now.
        if indexed_cols.len() != 1 {
            return Err(DbError::syntax("only single-column indexes are supported"));
        }
        let column_name = &indexed_cols[0].name;

        // ── Validate ────────────────────────────────────────────
        let mut schema = self.storage.load_schema()?;
        if schema.indexes.contains_key(&index_name) {
            if if_not_exists {
                return Ok(ExecutionResult::Message(format!(
                    "Index '{index_name}' already exists"
                )));
            }
            return Err(DbError::IndexExists(index_name));
        }

        let table_schema = schema.get_table(&table_name)?.clone();
        table_schema.column_index(column_name)?; // errors if column not found

        // ── Create index file ───────────────────────────────────
        self.storage.create_index_file(&index_name)?;

        // ── Populate from existing rows ─────────────────────────
        self.storage.scan_apply_rows(&table_name, |stored_row| {
            let col_index = table_schema.column_index(column_name)?;
            let val = &stored_row.values[col_index];
            self.storage
                .index_insert(&index_name, value_to_key(val), stored_row.row_id)
        })?;

        // ── Persist schema ──────────────────────────────────────
        schema.indexes.insert(
            index_name.clone(),
            IndexSchema {
                name: index_name.clone(),
                table_name,
                column: column_name.clone(),
            },
        );
        self.storage.save_schema(&schema)?;

        Ok(ExecutionResult::Message(format!(
            "Index '{index_name}' created on '{column_name}'"
        )))
    }

    // ── DROP INDEX ───────────────────────────────────────────────────

    pub(super) fn execute_drop_index(
        &mut self,
        drop_index: DropIndex,
    ) -> DbResult<ExecutionResult> {
        let DropIndex {
            if_exists,
            schema_index: SchemaObject {
                name: index_name, ..
            },
        } = drop_index;

        let mut schema = self.storage.load_schema()?;
        if schema.indexes.remove(&index_name).is_none() {
            if if_exists {
                return Ok(ExecutionResult::Message(format!(
                    "Index '{index_name}' does not exist"
                )));
            }
            return Err(DbError::IndexNotFound(index_name));
        }
        self.storage.save_schema(&schema)?;

        self.storage.remove_index_file(&index_name)?;

        Ok(ExecutionResult::Message(format!(
            "Index '{index_name}' dropped"
        )))
    }
}
