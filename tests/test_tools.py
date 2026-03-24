"""Integration tests for Berth MCP tools — requires database fixtures."""

import os
from unittest.mock import patch

import pytest

from berth import safety
from berth.connections import ConnectionManager
from berth.server import (
    _ensure_limit,
    _validate_backup_path,
    db_active_queries,
    db_backup,
    db_describe,
    db_execute,
    db_explain,
    db_query,
    db_relationships,
    db_restore,
    db_schema,
    db_size,
    health,
    safety_get_mode,
    safety_set_mode,
    mgr as server_mgr,
)


@pytest.fixture(autouse=True)
def _reset_safety():
    safety.reset()
    yield
    safety.reset()


@pytest.fixture
async def pg_conn(postgres_dsn):
    """Connect to Postgres and return connection_id. Uses the server's global manager."""
    conn = await server_mgr.connect(postgres_dsn)
    yield conn.conn_id
    await server_mgr.close(conn.conn_id)


@pytest.fixture
async def sqlite_conn():
    """Create an in-memory SQLite db with seed data and return connection_id."""
    conn = await server_mgr.connect(":memory:")
    await server_mgr.execute(
        conn.conn_id,
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT UNIQUE, role TEXT DEFAULT 'user')",
    )
    await server_mgr.execute(
        conn.conn_id,
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER REFERENCES users(id), "
        "product TEXT, amount REAL, status TEXT DEFAULT 'pending')",
    )
    for i in range(1, 11):
        await server_mgr.execute(
            conn.conn_id,
            f"INSERT INTO users (id, name, email, role) VALUES ({i}, 'User {i}', 'user{i}@test.local', 'user')",
        )
    for i in range(1, 51):
        uid = ((i - 1) % 10) + 1
        await server_mgr.execute(
            conn.conn_id,
            f"INSERT INTO orders (user_id, product, amount, status) VALUES ({uid}, 'Product {i}', {i * 10.0}, 'pending')",
        )
    yield conn.conn_id
    await server_mgr.close(conn.conn_id)


# ── Limit enforcement ──────────────────────────────────────────────────


class TestLimitEnforcement:
    def test_adds_limit_when_missing(self):
        result = _ensure_limit("SELECT * FROM users")
        assert "LIMIT 1000" in result

    def test_preserves_existing_limit(self):
        sql = "SELECT * FROM users LIMIT 5"
        assert _ensure_limit(sql) == sql

    def test_case_insensitive(self):
        sql = "SELECT * FROM users limit 10"
        assert _ensure_limit(sql) == sql


# ── Schema tools ────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_schema_returns_tables(sqlite_conn):
    result = await db_schema(sqlite_conn)
    assert "users" in result
    assert "orders" in result


@pytest.mark.asyncio
async def test_schema_postgres(pg_conn):
    result = await db_schema(pg_conn)
    assert "users" in result
    assert "orders" in result


# ── Query tools ─────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_query_with_limit(sqlite_conn):
    result = await db_query(sqlite_conn, "SELECT * FROM users LIMIT 3")
    lines = result.strip().split("\n")
    # header + separator + 3 data rows
    assert len(lines) == 5


@pytest.mark.asyncio
async def test_query_auto_limits(sqlite_conn):
    """Query without LIMIT should still return results (auto-limited to 1000)."""
    result = await db_query(sqlite_conn, "SELECT * FROM orders")
    lines = result.strip().split("\n")
    # 50 rows + header + separator
    assert len(lines) == 52


# ── Describe ────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_describe_returns_columns(sqlite_conn):
    result = await db_describe(sqlite_conn, "users")
    assert "name" in result
    assert "email" in result
    assert "role" in result


# ── Relationships ───────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_relationships_finds_fk(pg_conn):
    result = await db_relationships(pg_conn, "orders")
    assert "users" in result
    assert "user_id" in result


# ── Write safety ────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_execute_insert_blocked_readonly(sqlite_conn):
    result = await db_execute(
        sqlite_conn, "INSERT INTO users (name, email) VALUES ('new', 'new@test.local')"
    )
    assert "BLOCKED" in result
    assert "read-only" in result


@pytest.mark.asyncio
async def test_execute_insert_allowed_write_mode(sqlite_conn):
    safety.set_mode("write")
    result = await db_execute(
        sqlite_conn, "INSERT INTO users (name, email) VALUES ('new', 'new@test.local')"
    )
    assert "1 row(s) affected" in result


# ── SQL injection safety ───────────────────────────────────────────────


@pytest.mark.asyncio
async def test_sql_injection_in_query(sqlite_conn):
    """Injected SQL should not cause unintended side effects."""
    # This attempts injection via a subquery in a SELECT — should just return data
    result = await db_query(
        sqlite_conn, "SELECT * FROM users WHERE name = '' OR 1=1 --'"
    )
    # Should return rows (the query is valid SQL, just returns all rows)
    assert "User" in result


@pytest.mark.asyncio
async def test_write_injection_blocked_readonly(sqlite_conn):
    """An INSERT disguised as part of a statement should be blocked."""
    result = await db_execute(
        sqlite_conn,
        "INSERT INTO users (name, email) VALUES ('x', 'x@test.local'); DROP TABLE users; --",
    )
    assert "BLOCKED" in result


# ── Health tool ────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_health_returns_version_and_status():
    result = await health()
    assert "berth v" in result
    assert "status: ok" in result
    assert "mode:" in result


# ── db_size ────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_db_size_sqlite(sqlite_conn):
    result = await db_size(sqlite_conn)
    assert "Database size:" in result
    assert "bytes" in result


# ── db_explain security ───────────────────────────────────────────────


@pytest.mark.asyncio
async def test_explain_select_allowed(sqlite_conn):
    """EXPLAIN of a SELECT should succeed."""
    result = await db_explain(sqlite_conn, "SELECT * FROM users")
    # SQLite EXPLAIN QUERY PLAN returns row dicts — any non-error output is fine
    assert "BLOCKED" not in result


@pytest.mark.asyncio
async def test_explain_drop_blocked(sqlite_conn):
    """EXPLAIN of a DROP should be blocked by safety check."""
    result = await db_explain(sqlite_conn, "DROP TABLE users")
    assert "BLOCKED" in result


@pytest.mark.asyncio
async def test_explain_insert_blocked_readonly(sqlite_conn):
    """EXPLAIN of an INSERT should be blocked in read-only mode."""
    result = await db_explain(sqlite_conn, "INSERT INTO users (name, email) VALUES ('x', 'x@test.local')")
    assert "BLOCKED" in result


# ── db_backup / db_restore path validation ────────────────────────────


class TestBackupPathValidation:
    def test_relative_path_in_sandbox(self):
        valid, _ = _validate_backup_path("backup.sql")
        assert valid

    def test_dotdot_rejected(self):
        valid, err = _validate_backup_path("../../../etc/passwd")
        assert not valid
        assert "escapes" in err

    def test_absolute_outside_sandbox_rejected(self):
        valid, err = _validate_backup_path("/etc/passwd")
        assert not valid
        assert "escapes" in err

    def test_null_byte_rejected(self):
        valid, err = _validate_backup_path("backup\x00.sql")
        assert not valid
        assert "null" in err.lower()

    def test_absolute_inside_sandbox_allowed(self, tmp_path):
        target = str(tmp_path / "backup.sql")
        with patch.dict(os.environ, {"BERTH_BACKUP_DIR": str(tmp_path)}):
            valid, _ = _validate_backup_path(target)
            assert valid

    def test_traversal_with_symlink_components(self):
        """Dotdot traversal even with extra path components should be blocked."""
        valid, err = _validate_backup_path("subdir/../../etc/shadow")
        assert not valid
        assert "escapes" in err


@pytest.mark.asyncio
async def test_db_backup_path_traversal_blocked(sqlite_conn):
    """db_backup should reject path traversal attempts."""
    result = await db_backup(sqlite_conn, "../../../etc/evil.sql")
    assert "BLOCKED" in result


@pytest.mark.asyncio
async def test_db_restore_path_traversal_blocked(sqlite_conn):
    """db_restore should reject path traversal attempts."""
    safety.set_mode("admin")
    result = await db_restore(sqlite_conn, "../../../etc/evil.sql")
    assert "BLOCKED" in result


# ── SQLite PRAGMA injection ───────────────────────────────────────────


@pytest.mark.asyncio
async def test_describe_adversarial_table_name(sqlite_conn):
    """Adversarial table name should be rejected, not interpolated into PRAGMA."""
    result = await db_describe(sqlite_conn, "'); DROP TABLE users; --")
    assert "BLOCKED" in result
    # Verify users table still exists
    check = await db_query(sqlite_conn, "SELECT COUNT(*) AS cnt FROM users")
    assert "10" in check


@pytest.mark.asyncio
async def test_relationships_adversarial_table_name(sqlite_conn):
    """Adversarial table name should be rejected in db_relationships."""
    result = await db_relationships(sqlite_conn, "'); DROP TABLE users; --")
    assert "BLOCKED" in result


@pytest.mark.asyncio
async def test_describe_nonexistent_table(sqlite_conn):
    """Non-existent table should return a clear error."""
    result = await db_describe(sqlite_conn, "nonexistent_table")
    assert "BLOCKED" in result
    assert "does not exist" in result


# ── db_active_queries ──────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_active_queries_rejects_sqlite(sqlite_conn):
    """db_active_queries should reject non-Postgres connections."""
    result = await db_active_queries(sqlite_conn)
    assert "only supported for PostgreSQL" in result


# ── safety_set_mode / safety_get_mode ────────────────────────────────


@pytest.mark.asyncio
async def test_get_mode_default():
    """Default mode should be read-only."""
    result = await safety_get_mode()
    assert "read-only" in result


@pytest.mark.asyncio
async def test_set_mode_write():
    """Setting mode to write should return confirmation."""
    result = await safety_set_mode("write")
    assert "write" in result
    check = await safety_get_mode()
    assert "write" in check


@pytest.mark.asyncio
async def test_set_mode_admin():
    """Setting mode to admin should return confirmation."""
    result = await safety_set_mode("admin")
    assert "admin" in result
    check = await safety_get_mode()
    assert "admin" in check


@pytest.mark.asyncio
async def test_set_mode_readonly():
    """Setting mode to read-only should return confirmation."""
    safety.set_mode("admin")
    result = await safety_set_mode("read-only")
    assert "read-only" in result
    check = await safety_get_mode()
    assert "read-only" in check


@pytest.mark.asyncio
async def test_set_mode_invalid():
    """Invalid mode should return an error, not crash."""
    result = await safety_set_mode("superuser")
    assert "ERROR" in result
    assert "Invalid mode" in result
    # Mode should remain unchanged (read-only from autouse fixture)
    check = await safety_get_mode()
    assert "read-only" in check
