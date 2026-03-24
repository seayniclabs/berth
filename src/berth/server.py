"""Berth MCP server — database access tools.

Provides 12 tools for inspecting, querying, and managing databases
through the Model Context Protocol.
"""

import os
import re
import shutil
import subprocess

from mcp.server.fastmcp import FastMCP

from berth import __version__
from berth.connections import ConnectionManager, DBType
from berth.safety import (
    Mode,
    check_write_allowed,
    generate_confirmation_token,
    get_mode,
    is_destructive,
    set_mode,
    validate_confirmation_token,
)

mcp = FastMCP("berth")
mgr = ConnectionManager()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_LIMIT_RE = re.compile(r"\bLIMIT\s+\d+", re.IGNORECASE)


def _ensure_limit(sql: str, limit: int = 1000) -> str:
    """Add a LIMIT clause if one is not already present."""
    if _LIMIT_RE.search(sql):
        return sql
    return f"{sql.rstrip().rstrip(';')} LIMIT {limit}"


def _format_table(rows: list[dict]) -> str:
    """Format rows as a simple text table."""
    if not rows:
        return "(no rows)"
    cols = list(rows[0].keys())
    widths = {c: len(c) for c in cols}
    str_rows = []
    for row in rows:
        str_row = {c: str(row[c]) for c in cols}
        for c in cols:
            widths[c] = max(widths[c], len(str_row[c]))
        str_rows.append(str_row)

    header = " | ".join(c.ljust(widths[c]) for c in cols)
    sep = "-+-".join("-" * widths[c] for c in cols)
    lines = [header, sep]
    for sr in str_rows:
        lines.append(" | ".join(sr[c].ljust(widths[c]) for c in cols))
    return "\n".join(lines)


async def _validate_table_name(connection_id: str, table: str) -> tuple[bool, str]:
    """Validate that a table name exists in sqlite_master.

    Prevents SQL injection through table names used in PRAGMA statements.
    Returns (valid, error_message).
    """
    conn = mgr.get(connection_id)
    if conn.db_type != DBType.SQLITE:
        # For non-SQLite, table names are passed as parameters — no injection risk
        return True, ""

    # Check that the name exists as a real table/view in sqlite_master
    rows = await mgr.fetch(
        connection_id,
        "SELECT name FROM sqlite_master WHERE type IN ('table', 'view') AND name = ?",
        (table,),
    )
    if not rows:
        return False, f"Table '{table}' does not exist."
    return True, ""


def _validate_backup_path(path: str) -> tuple[bool, str]:
    """Validate a backup/restore path against traversal attacks.

    Rules:
    - No '..' components
    - Must resolve inside BERTH_BACKUP_DIR (default: cwd)
    - No null bytes

    Returns (valid, error_message).
    """
    if "\x00" in path:
        return False, "Path contains null bytes."

    backup_dir = os.environ.get("BERTH_BACKUP_DIR", os.getcwd())
    backup_dir = os.path.realpath(backup_dir)

    # Resolve the target path relative to backup_dir
    if os.path.isabs(path):
        resolved = os.path.realpath(path)
    else:
        resolved = os.path.realpath(os.path.join(backup_dir, path))

    if not resolved.startswith(backup_dir + os.sep) and resolved != backup_dir:
        return False, f"Path escapes the backup directory ({backup_dir})."

    return True, ""


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------


@mcp.tool()
async def safety_set_mode(mode: str) -> str:
    """Switch the safety mode.

    Accepts: "read-only", "write", or "admin".
    Returns the current mode after setting.
    """
    try:
        new_mode = set_mode(mode)
    except ValueError:
        return f"ERROR: Invalid mode '{mode}'. Valid modes: read-only, write, admin"
    return f"Mode set to: {new_mode.value}"


@mcp.tool()
async def safety_get_mode() -> str:
    """Return the current safety mode without changing it."""
    return f"Current mode: {get_mode().value}"


@mcp.tool()
async def health() -> str:
    """Server health check. Returns version and status."""
    return f"berth v{__version__} | status: ok | mode: {get_mode().value}"


@mcp.tool()
async def db_connect(dsn: str) -> str:
    """Connect to a database.

    Supported DSN formats:
    - postgresql://user:pass@host/db
    - sqlite:///path/to/file.db  (or :memory:)
    - mysql://user:pass@host/db

    Returns a connection_id used by all other tools.
    """
    conn = await mgr.connect(dsn)
    return (
        f"Connected: {conn.display_dsn}\n"
        f"connection_id: {conn.conn_id}\n"
        f"type: {conn.db_type.value}"
    )


@mcp.tool()
async def db_query(connection_id: str, sql: str) -> str:
    """Execute a SELECT query. Auto-adds LIMIT 1000 if no LIMIT clause present."""
    allowed, reason = check_write_allowed(sql)
    if not allowed:
        return f"BLOCKED: {reason}"

    safe_sql = _ensure_limit(sql)
    rows = await mgr.fetch(connection_id, safe_sql)
    return _format_table(rows)


@mcp.tool()
async def db_execute(
    connection_id: str, sql: str, confirmation_token: str | None = None
) -> str:
    """Execute INSERT/UPDATE/DELETE statements.

    Respects the current safety mode:
    - read-only: rejects all writes
    - write: allows INSERT/UPDATE/DELETE, blocks DROP/TRUNCATE
    - admin: allows everything (destructive ops need a confirmation_token)
    """
    allowed, reason = check_write_allowed(sql)
    if not allowed:
        return f"BLOCKED: {reason}"

    # Destructive ops in admin mode require confirmation
    if is_destructive(sql) and get_mode() == Mode.ADMIN:
        if confirmation_token is None:
            token = generate_confirmation_token()
            return (
                f"DESTRUCTIVE OPERATION DETECTED.\n"
                f"Re-run with confirmation_token: {token}\n"
                f"Token expires in 60 seconds."
            )
        if not validate_confirmation_token(confirmation_token):
            return "BLOCKED: Invalid or expired confirmation token."

    affected = await mgr.execute(connection_id, sql)
    return f"OK: {affected} row(s) affected"


@mcp.tool()
async def db_schema(connection_id: str) -> str:
    """List tables, views, and indexes in the database."""
    conn = mgr.get(connection_id)
    results: list[str] = []

    if conn.db_type == DBType.POSTGRES:
        tables = await mgr.fetch(
            connection_id,
            "SELECT table_name, table_type FROM information_schema.tables "
            "WHERE table_schema = 'public' ORDER BY table_type, table_name",
        )
        results.append("== Tables & Views ==")
        for t in tables:
            results.append(f"  {t['table_name']} ({t['table_type']})")

        indexes = await mgr.fetch(
            connection_id,
            "SELECT indexname, tablename FROM pg_indexes "
            "WHERE schemaname = 'public' ORDER BY tablename, indexname",
        )
        results.append("\n== Indexes ==")
        for idx in indexes:
            results.append(f"  {idx['indexname']} ON {idx['tablename']}")

    elif conn.db_type == DBType.SQLITE:
        tables = await mgr.fetch(
            connection_id,
            "SELECT name, type FROM sqlite_master "
            "WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%' "
            "ORDER BY type, name",
        )
        results.append("== Tables & Views ==")
        for t in tables:
            results.append(f"  {t['name']} ({t['type']})")

        indexes = await mgr.fetch(
            connection_id,
            "SELECT name, tbl_name FROM sqlite_master "
            "WHERE type = 'index' AND name NOT LIKE 'sqlite_%' ORDER BY tbl_name, name",
        )
        results.append("\n== Indexes ==")
        for idx in indexes:
            results.append(f"  {idx['name']} ON {idx['tbl_name']}")

    elif conn.db_type == DBType.MYSQL:
        tables = await mgr.fetch(
            connection_id,
            "SELECT table_name, table_type FROM information_schema.tables "
            "WHERE table_schema = DATABASE() ORDER BY table_type, table_name",
        )
        results.append("== Tables & Views ==")
        for t in tables:
            col = "table_name" if "table_name" in t else "TABLE_NAME"
            typ = "table_type" if "table_type" in t else "TABLE_TYPE"
            results.append(f"  {t[col]} ({t[typ]})")

        indexes = await mgr.fetch(
            connection_id,
            "SELECT DISTINCT index_name, table_name FROM information_schema.statistics "
            "WHERE table_schema = DATABASE() ORDER BY table_name, index_name",
        )
        results.append("\n== Indexes ==")
        for idx in indexes:
            icol = "index_name" if "index_name" in idx else "INDEX_NAME"
            tcol = "table_name" if "table_name" in idx else "TABLE_NAME"
            results.append(f"  {idx[icol]} ON {idx[tcol]}")

    return "\n".join(results)


@mcp.tool()
async def db_describe(connection_id: str, table: str) -> str:
    """Column details for a table: name, type, nullable, default, constraints."""
    conn = mgr.get(connection_id)
    rows: list[dict] = []

    if conn.db_type == DBType.POSTGRES:
        rows = await mgr.fetch(
            connection_id,
            "SELECT column_name, data_type, is_nullable, column_default "
            "FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = $1 "
            "ORDER BY ordinal_position",
            (table,),
        )

    elif conn.db_type == DBType.SQLITE:
        valid, err = await _validate_table_name(connection_id, table)
        if not valid:
            return f"BLOCKED: {err}"
        raw = await mgr.fetch(connection_id, f"PRAGMA table_info('{table}')")
        rows = [
            {
                "column_name": r["name"],
                "data_type": r["type"],
                "is_nullable": "YES" if not r["notnull"] else "NO",
                "column_default": r["dflt_value"],
            }
            for r in raw
        ]

    elif conn.db_type == DBType.MYSQL:
        rows = await mgr.fetch(
            connection_id,
            "SELECT column_name, data_type, is_nullable, column_default "
            "FROM information_schema.columns "
            "WHERE table_schema = DATABASE() AND table_name = %s "
            "ORDER BY ordinal_position",
            (table,),
        )

    return _format_table(rows)


@mcp.tool()
async def db_relationships(
    connection_id: str, table: str | None = None
) -> str:
    """Foreign key relationships. Omit table to show all."""
    conn = mgr.get(connection_id)
    rows: list[dict] = []

    if conn.db_type == DBType.POSTGRES:
        sql = (
            "SELECT "
            "  tc.table_name AS source_table, "
            "  kcu.column_name AS source_column, "
            "  ccu.table_name AS target_table, "
            "  ccu.column_name AS target_column "
            "FROM information_schema.table_constraints tc "
            "JOIN information_schema.key_column_usage kcu "
            "  ON tc.constraint_name = kcu.constraint_name "
            "JOIN information_schema.constraint_column_usage ccu "
            "  ON tc.constraint_name = ccu.constraint_name "
            "WHERE tc.constraint_type = 'FOREIGN KEY'"
        )
        params: tuple = ()
        if table:
            sql += " AND tc.table_name = $1"
            params = (table,)
        rows = await mgr.fetch(connection_id, sql, params)

    elif conn.db_type == DBType.SQLITE:
        # SQLite: need to query each table individually
        if table:
            valid, err = await _validate_table_name(connection_id, table)
            if not valid:
                return f"BLOCKED: {err}"
            tables_to_check = [table]
        else:
            t_rows = await mgr.fetch(
                connection_id,
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
            )
            tables_to_check = [r["name"] for r in t_rows]

        for tbl in tables_to_check:
            # tables_to_check entries come from sqlite_master — already validated
            fks = await mgr.fetch(connection_id, f"PRAGMA foreign_key_list('{tbl}')")
            for fk in fks:
                rows.append(
                    {
                        "source_table": tbl,
                        "source_column": fk["from"],
                        "target_table": fk["table"],
                        "target_column": fk["to"],
                    }
                )

    elif conn.db_type == DBType.MYSQL:
        sql = (
            "SELECT "
            "  TABLE_NAME AS source_table, "
            "  COLUMN_NAME AS source_column, "
            "  REFERENCED_TABLE_NAME AS target_table, "
            "  REFERENCED_COLUMN_NAME AS target_column "
            "FROM information_schema.KEY_COLUMN_USAGE "
            "WHERE TABLE_SCHEMA = DATABASE() AND REFERENCED_TABLE_NAME IS NOT NULL"
        )
        params_m: tuple = ()
        if table:
            sql += " AND TABLE_NAME = %s"
            params_m = (table,)
        rows = await mgr.fetch(connection_id, sql, params_m)

    if not rows:
        return "(no foreign key relationships found)"
    return _format_table(rows)


@mcp.tool()
async def db_size(connection_id: str) -> str:
    """Database and table sizes."""
    conn = mgr.get(connection_id)
    results: list[str] = []

    if conn.db_type == DBType.POSTGRES:
        db_size = await mgr.fetch(
            connection_id,
            "SELECT pg_size_pretty(pg_database_size(current_database())) AS size",
        )
        results.append(f"Database size: {db_size[0]['size']}")
        table_sizes = await mgr.fetch(
            connection_id,
            "SELECT tablename, "
            "pg_size_pretty(pg_total_relation_size(quote_ident(tablename))) AS size "
            "FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename",
        )
        results.append("\nTable sizes:")
        for t in table_sizes:
            results.append(f"  {t['tablename']}: {t['size']}")

    elif conn.db_type == DBType.SQLITE:
        page_info = await mgr.fetch(
            connection_id, "PRAGMA page_count"
        )
        page_size = await mgr.fetch(
            connection_id, "PRAGMA page_size"
        )
        count = list(page_info[0].values())[0] if page_info else 0
        size = list(page_size[0].values())[0] if page_size else 0
        total = int(count) * int(size)
        results.append(f"Database size: {total} bytes")

    elif conn.db_type == DBType.MYSQL:
        db_size = await mgr.fetch(
            connection_id,
            "SELECT table_name, "
            "ROUND((data_length + index_length) / 1024, 2) AS size_kb "
            "FROM information_schema.tables "
            "WHERE table_schema = DATABASE() ORDER BY table_name",
        )
        results.append("Table sizes:")
        for t in db_size:
            col = "table_name" if "table_name" in t else "TABLE_NAME"
            scol = "size_kb" if "size_kb" in t else "SIZE_KB"
            results.append(f"  {t[col]}: {t[scol]} KB")

    return "\n".join(results)


@mcp.tool()
async def db_active_queries(connection_id: str) -> str:
    """Currently running queries (PostgreSQL only — pg_stat_activity)."""
    conn = mgr.get(connection_id)

    if conn.db_type != DBType.POSTGRES:
        return "db_active_queries is only supported for PostgreSQL."

    rows = await mgr.fetch(
        connection_id,
        "SELECT pid, state, query_start::text, left(query, 200) AS query "
        "FROM pg_stat_activity "
        "WHERE state != 'idle' AND pid != pg_backend_pid() "
        "ORDER BY query_start",
    )
    if not rows:
        return "(no active queries)"
    return _format_table(rows)


@mcp.tool()
async def db_explain(connection_id: str, sql: str) -> str:
    """Run EXPLAIN ANALYZE on a query and return the plan."""
    # Validate the inner SQL before prepending EXPLAIN — prevents injection
    allowed, reason = check_write_allowed(sql)
    if not allowed:
        return f"BLOCKED: {reason}"

    conn = mgr.get(connection_id)

    if conn.db_type == DBType.POSTGRES:
        rows = await mgr.fetch(
            connection_id, f"EXPLAIN ANALYZE {sql}"
        )
        return "\n".join(r.get("QUERY PLAN", str(r)) for r in rows)

    elif conn.db_type == DBType.SQLITE:
        rows = await mgr.fetch(
            connection_id, f"EXPLAIN QUERY PLAN {sql}"
        )
        return "\n".join(str(r) for r in rows)

    elif conn.db_type == DBType.MYSQL:
        rows = await mgr.fetch(
            connection_id, f"EXPLAIN {sql}"
        )
        return _format_table(rows)

    return "Unsupported database type for EXPLAIN."


@mcp.tool()
async def db_backup(connection_id: str, output_path: str) -> str:
    """Create a database backup.

    - PostgreSQL: uses pg_dump
    - MySQL: uses mysqldump
    - SQLite: uses .backup via sqlite3 CLI

    Paths are sandboxed to BERTH_BACKUP_DIR (default: cwd).
    """
    valid, err = _validate_backup_path(output_path)
    if not valid:
        return f"BLOCKED: {err}"

    conn = mgr.get(connection_id)

    if conn.db_type == DBType.POSTGRES:
        tool = shutil.which("pg_dump")
        if not tool:
            return "ERROR: pg_dump not found on PATH"
        result = subprocess.run(
            [tool, conn.dsn, "-f", output_path],
            capture_output=True,
            text=True,
            timeout=300,
        )
        if result.returncode != 0:
            return f"ERROR: {result.stderr}"
        return f"Backup saved to {output_path}"

    elif conn.db_type == DBType.SQLITE:
        from urllib.parse import urlparse

        path = conn.dsn.replace("sqlite:///", "") if conn.dsn.startswith("sqlite:") else conn.dsn
        if path == ":memory:":
            return "ERROR: Cannot backup an in-memory database"
        tool = shutil.which("sqlite3")
        if not tool:
            return "ERROR: sqlite3 not found on PATH"
        result = subprocess.run(
            [tool, path, f".backup '{output_path}'"],
            capture_output=True,
            text=True,
            timeout=300,
        )
        if result.returncode != 0:
            return f"ERROR: {result.stderr}"
        return f"Backup saved to {output_path}"

    elif conn.db_type == DBType.MYSQL:
        from urllib.parse import urlparse

        parsed = urlparse(conn.dsn)
        tool = shutil.which("mysqldump")
        if not tool:
            return "ERROR: mysqldump not found on PATH"
        cmd = [
            tool,
            f"--host={parsed.hostname or 'localhost'}",
            f"--port={parsed.port or 3306}",
            f"--user={parsed.username or 'root'}",
            f"--password={parsed.password or ''}",
            parsed.path.lstrip("/"),
            f"--result-file={output_path}",
        ]
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=300
        )
        if result.returncode != 0:
            return f"ERROR: {result.stderr}"
        return f"Backup saved to {output_path}"

    return "Unsupported database type for backup."


@mcp.tool()
async def db_restore(
    connection_id: str, input_path: str, confirmation_token: str | None = None
) -> str:
    """Restore a database from a backup file.

    Requires admin mode and a confirmation token (destructive operation).
    Paths are sandboxed to BERTH_BACKUP_DIR (default: cwd).
    """
    valid, err = _validate_backup_path(input_path)
    if not valid:
        return f"BLOCKED: {err}"

    if get_mode() != Mode.ADMIN:
        return "BLOCKED: db_restore requires admin mode. Use set_mode('admin') first."

    if confirmation_token is None:
        token = generate_confirmation_token()
        return (
            f"DESTRUCTIVE OPERATION: database restore will overwrite data.\n"
            f"Re-run with confirmation_token: {token}\n"
            f"Token expires in 60 seconds."
        )
    if not validate_confirmation_token(confirmation_token):
        return "BLOCKED: Invalid or expired confirmation token."

    conn = mgr.get(connection_id)

    if conn.db_type == DBType.POSTGRES:
        tool = shutil.which("psql")
        if not tool:
            return "ERROR: psql not found on PATH"
        result = subprocess.run(
            [tool, conn.dsn, "-f", input_path],
            capture_output=True,
            text=True,
            timeout=300,
        )
        if result.returncode != 0:
            return f"ERROR: {result.stderr}"
        return f"Restored from {input_path}"

    elif conn.db_type == DBType.SQLITE:
        path = conn.dsn.replace("sqlite:///", "") if conn.dsn.startswith("sqlite:") else conn.dsn
        tool = shutil.which("sqlite3")
        if not tool:
            return "ERROR: sqlite3 not found on PATH"
        result = subprocess.run(
            [tool, path, f".restore '{input_path}'"],
            capture_output=True,
            text=True,
            timeout=300,
        )
        if result.returncode != 0:
            return f"ERROR: {result.stderr}"
        return f"Restored from {input_path}"

    elif conn.db_type == DBType.MYSQL:
        from urllib.parse import urlparse

        parsed = urlparse(conn.dsn)
        tool = shutil.which("mysql")
        if not tool:
            return "ERROR: mysql not found on PATH"
        cmd = [
            tool,
            f"--host={parsed.hostname or 'localhost'}",
            f"--port={parsed.port or 3306}",
            f"--user={parsed.username or 'root'}",
            f"--password={parsed.password or ''}",
            parsed.path.lstrip("/"),
        ]
        with open(input_path, "r") as f:
            result = subprocess.run(
                cmd, stdin=f, capture_output=True, text=True, timeout=300
            )
        if result.returncode != 0:
            return f"ERROR: {result.stderr}"
        return f"Restored from {input_path}"

    return "Unsupported database type for restore."


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


def main() -> None:
    """Run the Berth MCP server."""
    mcp.run()


if __name__ == "__main__":
    main()
