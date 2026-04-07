# Berth -- Database MCP Server

[![License: MIT](https://img.shields.io/badge/License-MIT-34D399.svg)](LICENSE)

A secure berth for your data -- database access for AI tools.

Berth is a [Model Context Protocol](https://modelcontextprotocol.io/) server that gives AI assistants safe, structured access to PostgreSQL, SQLite, and MySQL databases. It exposes 13 tools for inspecting schemas, running queries, managing data, generating migrations, and performing backups -- all governed by a 3-tier safety model that prevents accidental damage.

---

## Safety Model

Berth enforces three operating modes that control what SQL is permitted:

| Mode | Default | Allows | Blocks |
|------|---------|--------|--------|
| **read-only** | Yes | `SELECT`, `EXPLAIN` | All writes |
| **write** | No | `INSERT`, `UPDATE`, `DELETE`, `CREATE` | `DROP`, `TRUNCATE`, `ALTER DROP`, `DELETE` without `WHERE` |
| **admin** | No | Everything | Destructive ops require a confirmation token (60s expiry) |

The server starts in **read-only mode**. Write and admin modes must be explicitly enabled. Destructive operations in admin mode generate a one-time confirmation token that expires after 60 seconds -- the AI must echo the token back to confirm intent.

---

## Tools

| Tool | Description | Key Parameters |
|------|-------------|----------------|
| `health` | Server health check | -- |
| `db_connect` | Connect to a database | `dsn` (connection string) |
| `db_query` | Execute a SELECT query (auto-adds LIMIT 1000) | `connection_id`, `sql` |
| `db_execute` | Execute INSERT/UPDATE/DELETE (respects safety mode) | `connection_id`, `sql`, `confirmation_token` |
| `db_schema` | List tables, views, and indexes | `connection_id` |
| `db_describe` | Column details for a table | `connection_id`, `table` |
| `db_relationships` | Foreign key relationships | `connection_id`, `table` (optional) |
| `db_size` | Database and table sizes | `connection_id` |
| `db_active_queries` | Currently running queries (PostgreSQL only) | `connection_id` |
| `db_explain` | Run EXPLAIN ANALYZE on a query | `connection_id`, `sql` |
| `generate_migration` | Generate migration SQL by comparing schemas | `connection_id` + `target_sql`, or `from_connection` + `to_connection` |
| `db_backup` | Create a database backup | `connection_id`, `output_path` |
| `db_restore` | Restore from backup (admin mode + confirmation token) | `connection_id`, `input_path`, `confirmation_token` |

---

## Schema Migrations

The `generate_migration` tool compares two schemas and produces dialect-aware SQL to migrate from one to the other. Two modes of operation:

**Mode 1 — Live database vs. target DDL:**

Provide `connection_id` (an active connection) and `target_sql` (CREATE TABLE statements describing the desired schema). Berth introspects the live database and diffs it against the parsed target.

**Mode 2 — Two live databases:**

Provide `from_connection` and `to_connection` (two active connection IDs). Berth introspects both and generates the migration to transform the source into the target.

**What it generates:**

- `CREATE TABLE` for new tables
- `ALTER TABLE ADD COLUMN` for new columns
- `ALTER TABLE ALTER COLUMN` / `MODIFY COLUMN` for type, nullability, and default changes
- `CREATE INDEX` / `DROP INDEX` for index changes
- `ADD CONSTRAINT` / `DROP CONSTRAINT` for foreign key changes
- `DROP TABLE` and `DROP COLUMN` are commented out with warnings (safety first)

**Dialect handling:**

- **PostgreSQL** -- uses `ALTER COLUMN ... TYPE`, `SET/DROP NOT NULL`, `SET/DROP DEFAULT`
- **MySQL** -- uses `MODIFY COLUMN` for all column changes, `DROP INDEX ... ON table`
- **SQLite** -- warns about unsupported operations and includes the table rebuild pattern for changes that require it (ALTER COLUMN, DROP COLUMN on older versions, constraint changes)

---

## Supported Databases

- **PostgreSQL** -- full support including `pg_stat_activity`, `EXPLAIN ANALYZE`, `pg_dump`/`psql` backup/restore
- **SQLite** -- full support including PRAGMA introspection, `.backup`/`.restore` via `sqlite3` CLI
- **MySQL** -- full support including `information_schema` introspection, `mysqldump`/`mysql` backup/restore

---

## Installation

From PyPI:

```bash
pip install berth-mcp
```

Or in an isolated environment:

```bash
pipx install berth-mcp
```

MySQL support requires an optional dependency:

```bash
pip install berth-mcp[mysql]
```

PostgreSQL (`asyncpg`) and SQLite (`aiosqlite`) drivers are included by default.

---

## Usage

Run the server:

```bash
berth
```

Berth communicates over stdio using the MCP protocol. It is designed to be launched by an MCP client, not run standalone.

### Claude Code

```bash
claude mcp add berth -- berth
```

### Claude Desktop

Add to your `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "berth": {
      "command": "berth",
      "args": []
    }
  }
}
```

If installed in a virtual environment, use the full path:

```json
{
  "mcpServers": {
    "berth": {
      "command": "/path/to/venv/bin/berth",
      "args": []
    }
  }
}
```

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `BERTH_BACKUP_DIR` | Current working directory | Sandbox directory for backup and restore paths. All paths are validated to stay within this directory. |

---

## Security

- **3-tier safety model** -- read-only by default, writes require explicit opt-in, destructive ops require confirmation tokens
- **Confirmation tokens** -- one-time UUIDs with 60-second expiry for DROP, TRUNCATE, ALTER DROP, and full-table DELETE
- **SQL injection protection** -- table names validated against `sqlite_master` before use in PRAGMA statements; parameterized queries used throughout
- **Path traversal protection** -- backup/restore paths are resolved and validated to stay within `BERTH_BACKUP_DIR`; null bytes rejected
- **Password masking** -- DSN passwords are masked in all display output and error messages

---

## Development

```bash
git clone https://github.com/seayniclabs/berth.git
cd berth
python -m venv .venv && source .venv/bin/activate
pip install -e ".[test]"
python -m pytest tests/ -q
```

Integration tests for PostgreSQL and MySQL require Docker:

```bash
docker compose -f tests/docker-compose.test.yml up -d
python -m pytest tests/ -q
docker compose -f tests/docker-compose.test.yml down
```

---

## License

[MIT](LICENSE)

<!-- mcp-name: io.github.seayniclabs/berth -->
