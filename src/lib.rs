pub mod engine;
pub mod error;
pub mod schema;

mod storage;
mod wal;

/// Re-export of the core execution engine.
///
/// This is the primary programmatic entrypoint for embedding the mini DBMS.
pub use crate::engine::Engine as DbEngine;

/// Re-export of statement execution results returned by [`Engine::execute`].
pub use crate::engine::ExecutionResult as DbExecutionResult;

/// Re-export of the unified DBMS error type.
pub use crate::error::DbError;

/// Re-export of the common DBMS result alias.
pub use crate::error::DbResult;

/// Re-export of runtime cell values used in row results.
pub use crate::schema::Value as DbValue;
