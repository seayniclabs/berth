"""Integration tests for Berth MCP tools — requires database fixtures."""

import pytest

from berth import safety
from berth.connections import ConnectionManager
from berth.server import (
    _ensure_limit,
    db_describe,
    db_execute,
    db_query,
    db_relationships,
    db_schema,
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
