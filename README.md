# Simple DBMS

A teaching-oriented mini DBMS implemented in Rust.

This project is designed to help you understand how a database engine works from the inside by implementing a small but functional SQL execution engine, file-based storage layer, schema manager, parser integration, and transaction rollback with WAL.


## Introduce

`simple_dbms` is a lightweight single-process, file-based database system written in Rust.

It is **not** intended to compete with production databases such as SQLite, MySQL, or PostgreSQL. Instead, it focuses on clarity, modularity, and educational value. The codebase demonstrates how core DBMS concepts can be built step by step, including:

- schema definition and persistence
- table data storage on disk
- SQL parsing and execution
- type checking and constraint validation
- row-based querying and filtering
- transactional rollback using undo-style WAL
- startup recovery for unfinished transactions

The system uses local files for persistence:

- `schema.json` stores metadata
- each table is stored as its own JSONL file
- `wal.log` stores transaction undo records


## Feature

- **Parser**: uses `pesqlite` for SQL parsing into AST
- **SQL support**: basic DDL, DML and queries with `WHERE`
- **Transactions**: undo-style WAL with rollback support
- **Persistence**: based on JSON/JSONL files for schema and table data


## Project Structure

A simplified view of the project layout:

- `src/engine/` — SQL execution logic
- `src/storage/` — file storage and row persistence
- `src/error.rs` — unified error handling
- `src/schema.rs` — schema and value definitions
- `src/wal.rs` — WAL record definitions and log file management
- `src/main.rs` — CLI entrypoint
- `src/lib.rs` — public API interfaces


## Usage

### Run the REPL

```sh
cargo run -- --root ./data
```

If `--root` is omitted, the engine uses its default storage directory behavior defined by the CLI.

In the REPL:
- finish SQL statements with `;`
- type `\q` on an empty prompt to quit

Example:

```text
simple-db> CREATE TABLE users (id INT, name VARCHAR(10));
Table 'users' created
simple-db> INSERT INTO users VALUES (1, 'alice');
1 row(s) inserted into 'users'
simple-db> SELECT * FROM users;
id | name
1 | alice
```


## Current Limitations

This is a teaching DBMS, so several limitations are intentional.

### SQL limitations
Not currently supported:

- joins
- subqueries in execution paths
- `ORDER BY`
- `GROUP BY`
- `HAVING`
- `LIMIT / OFFSET`
- `CREATE INDEX`
- `CREATE VIEW`
- `CREATE TRIGGER`
- `DROP INDEX`
- `DROP VIEW`
- `DROP TRIGGER`
- `INSERT INTO ... SELECT ...`
- `RETURNING`
- savepoints (`SAVEPOINT`, `RELEASE`, `ROLLBACK TO`)

### Runtime limitations
- single-process / single-connection design
- no query optimizer
- no indexing
- no multiple databases/catalog switching
- no durability tuning such as fsync policy management

### Storage limitations
- JSON/JSONL is used for clarity instead of performance
- full-table rewrites still occur in some paths
- no page manager or buffer pool
