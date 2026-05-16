//! Value-to-key conversion shared by constraints and index.
//!
//! Collisions are possible for STRING values (via djb2 hash). Callers must
//! verify equality by reading the actual row after a key-based lookup.

use crate::schema::Value;

/// Convert a `Value` into an `i64` key for hashing or B-Tree indexing.
///
/// * Int → direct
/// * Float → u64 bit pattern cast
/// * Str → djb2 hash
/// * Null → 0
pub fn value_to_key(v: &Value) -> i64 {
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
