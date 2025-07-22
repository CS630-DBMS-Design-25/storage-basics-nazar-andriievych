# Storage Basics: SQL and Storage Layer Implementation

## Overview

This project implements a basic database management system with a custom storage layer and a SQL execution engine. It provides two command-line interfaces (CLIs):
- **Storage CLI**: Directly interacts with the storage layer for low-level operations.
- **SQL CLI**: Supports a subset of SQL-92 for higher-level database operations.

The system is designed for educational purposes, demonstrating how a database parses, stores, and retrieves data using custom file-based storage.

---

## Project Structure

- `src/` — Source code for the storage layer, SQL engine, and CLIs
- `include/` — Header files for the main components
- `tests/` — Unit tests for storage and SQL functionality
- `docs/` — Documentation and design notes
- `CMakeLists.txt` — Build configuration

---

## Architecture

### 1. Storage Layer
- **FileStorageLayer**: Manages tables, pages, and records on disk.
- **Page**: Represents a fixed-size block containing records, with support for insertion, update, deletion, and compaction.
- **CatalogPage**: Stores metadata about tables and their schemas.
- **TableMetadata**: Describes a table's schema, data pages, and record count.
- **Serialization/Deserialization**: Records are serialized into bytes for storage and deserialized for retrieval.

### 2. SQL Engine
- **SqlLexer**: Tokenizes SQL input.
- **SqlParser**: Parses tokens into an abstract syntax tree (AST).
- **SqlExecutor**: Executes ASTs by translating them into storage layer operations.
- **Supported SQL**: Subset of SQL-92, including `CREATE TABLE`, `INSERT`, `DELETE`, `SELECT`, `JOIN`, `ORDER BY`, `LIMIT`, `SUM`, and `ABS`.

### 3. Command-Line Interfaces
- **storage_cli**: Directly manipulates tables and records using custom commands.
- **sql_cli**: Accepts SQL queries and translates them into storage operations.

---

## Usage

### Building

Use CMake to build the project:
```sh
mkdir build && cd build
cmake ..
make
```

### Running

#### 1. Storage CLI
```sh
./storage_cli
```

**Available Commands:**
- `open <path>` — Open storage at specified path
- `close` — Close the storage
- `create <table> <col1>:<type1> ...` — Create a table with schema
- `insert <table> <val1,val2,...>` — Insert a record
- `get <table> <record_id>` — Get a record by ID
- `update <table> <record_id> <val1,val2,...>` — Update a record
- `delete <table> <record_id>` — Delete a record
- `scan <table> [options]` — Scan records in a table
    - `--projection <field1> <field2> ...`
    - `--where <col>=<val>`
    - `--orderby <col>[:asc|desc] ...`
    - `--limit <N>`
    - `--aggregate <SUM|ABS>:<col>`
- `flush` — Flush data to disk
- `help` — Show help
- `exit` / `quit` — Exit the CLI

#### 2. SQL CLI
```sh
./sql_cli
```

**Supported SQL Syntax:**
- `CREATE TABLE table (col1 TYPE, col2 TYPE, ...);`
- `INSERT INTO table VALUES (val1, val2, ...);`
- `DELETE FROM table [WHERE col = val [AND ...]];`
- `SELECT col1, col2 FROM table [WHERE col = val [AND ...]] [ORDER BY col [ASC|DESC]] [LIMIT N];`
- `SELECT * FROM table ...`
- `SELECT SUM(col) FROM table ...`
- `SELECT ... FROM t1 JOIN t2 ON t1.col = t2.col ...`
- `SELECT ABS(col) FROM table ...`

**Special Commands:**
- `help` — Show SQL help
- `exit` / `quit` — Exit the CLI
- `AST ON` / `AST OFF` — Enable/disable AST printing

---

## Example Workflow

**Storage CLI:**
```sh
./storage_cli
create users id:INT name:TEXT
insert users 1,John
scan users
```

**SQL CLI:**
```sql
./sql_cli
CREATE TABLE users (id INT, name TEXT);
INSERT INTO users VALUES (1, 'John');
SELECT * FROM users;
```

---

## Design Notes

- **Persistence**: Data is stored in binary files, with a catalog for metadata and pages for records.
- **Extensibility**: The modular design allows for future extension (e.g., more SQL features, new data types).
- **Testing**: See the `tests/` directory for unit tests covering storage and SQL operations.

---

## Authors

- **Nazar Andriievych** — [GitHub Profile](https://github.com/nazar-andriievych)
- Special thanks to **Denys Tsomenko**, instructor of Database Management Systems Design, for the task, support, guidance, and invaluable knowledge.

---

## License

MIT License

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY.
