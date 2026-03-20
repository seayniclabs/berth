"""Integration tests for berth.connections — requires database fixtures."""

import pytest

from berth.connections import ConnectionManager, _mask_password


@pytest.fixture
async def manager():
    mgr = ConnectionManager()
    yield mgr
    await mgr.close_all()


# ── Connection tests ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_connect_postgres(manager, postgres_dsn):
    conn = await manager.connect(postgres_dsn)
    assert conn.conn_id
    assert conn.db_type.value == "postgres"
    assert await manager.health_check(conn.conn_id)


@pytest.mark.asyncio
async def test_connect_sqlite(manager, sqlite_dsn):
    conn = await manager.connect(sqlite_dsn)
    assert conn.conn_id
    assert conn.db_type.value == "sqlite"


@pytest.mark.asyncio
async def test_connect_invalid_dsn_gives_clear_error(manager):
    with pytest.raises(ConnectionError, match="Failed to connect"):
        await manager.connect("postgresql://bad:bad@localhost:19999/nope")


@pytest.mark.asyncio
async def test_reconnect_returns_same_id(manager, sqlite_dsn):
    c1 = await manager.connect(sqlite_dsn)
    c2 = await manager.connect(sqlite_dsn)
    assert c1.conn_id == c2.conn_id


# ── Password masking ───────────────────────────────────────────────────


class TestPasswordMasking:
    def test_postgres_dsn(self):
        assert _mask_password("postgresql://user:secret@host/db") == \
            "postgresql://user:***@host/db"

    def test_mysql_dsn(self):
        assert _mask_password("mysql://root:hunter2@localhost/mydb") == \
            "mysql://root:***@localhost/mydb"

    def test_no_password(self):
        dsn = "sqlite:///path/to/db.sqlite"
        assert _mask_password(dsn) == dsn

    def test_password_not_in_error_output(self, postgres_dsn):
        """Verify masked DSN doesn't contain the actual password."""
        masked = _mask_password(postgres_dsn)
        assert "testpass" not in masked
