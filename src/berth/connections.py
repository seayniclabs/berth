"""Connection management for Berth.

Manages async connection pools for PostgreSQL, SQLite, and MySQL.
Detects database type from DSN scheme and provides unified execute/fetch interface.
"""

import hashlib
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any
from urllib.parse import urlparse


class DBType(str, Enum):
    POSTGRES = "postgres"
    SQLITE = "sqlite"
    MYSQL = "mysql"


@dataclass
class Connection:
    """Wrapper around a database connection/pool."""

    conn_id: str
    db_type: DBType
    dsn: str
    pool: Any = None  # asyncpg.Pool | aiosqlite.Connection | aiomysql.Pool
    _display_dsn: str = field(default="", repr=False)

    @property
    def display_dsn(self) -> str:
        """DSN with password masked for safe display."""
        return self._display_dsn or self.dsn


def _mask_password(dsn: str) -> str:
    """Replace password in a DSN with '***'."""
    return re.sub(r"(://[^:]+:)[^@]+(@)", r"\1***\2", dsn)


def _detect_db_type(dsn: str) -> DBType:
    """Detect database type from DSN scheme."""
    if dsn == ":memory:" or dsn.startswith("sqlite"):
        return DBType.SQLITE
    parsed = urlparse(dsn)
    scheme = parsed.scheme.lower().split("+")[0]
    if scheme in ("postgresql", "postgres"):
        return DBType.POSTGRES
    if scheme == "mysql":
        return DBType.MYSQL
    if scheme == "sqlite":
        return DBType.SQLITE
    raise ValueError(f"Unsupported database scheme: {scheme}")


def _conn_id(dsn: str) -> str:
    """Generate a stable connection ID from a DSN."""
    return hashlib.sha256(dsn.encode()).hexdigest()[:12]


class ConnectionManager:
    """Manages async database connection pools keyed by DSN hash."""

    def __init__(self) -> None:
        self._connections: dict[str, Connection] = {}

    async def connect(self, dsn: str) -> Connection:
        """Connect to a database. Returns existing connection if already connected."""
        conn_id = _conn_id(dsn)
        if conn_id in self._connections:
            return self._connections[conn_id]

        db_type = _detect_db_type(dsn)
        masked = _mask_password(dsn)

        try:
            pool = await self._create_pool(dsn, db_type)
        except Exception as exc:
            # Ensure no passwords leak in error messages
            safe_msg = str(exc)
            safe_msg = _mask_password(safe_msg)
            raise ConnectionError(
                f"Failed to connect to {masked}: {safe_msg}"
            ) from None

        conn = Connection(
            conn_id=conn_id,
            db_type=db_type,
            dsn=dsn,
            pool=pool,
            _display_dsn=masked,
        )
        self._connections[conn_id] = conn
        return conn

    async def _create_pool(self, dsn: str, db_type: DBType) -> Any:
        """Create the appropriate connection pool."""
        if db_type == DBType.POSTGRES:
            import asyncpg

            return await asyncpg.create_pool(dsn, min_size=1, max_size=5)

        if db_type == DBType.SQLITE:
            import aiosqlite

            path = dsn.replace("sqlite:///", "") if dsn.startswith("sqlite:") else dsn
            conn = await aiosqlite.connect(path)
            conn.row_factory = aiosqlite.Row
            return conn

        if db_type == DBType.MYSQL:
            import aiomysql
            from urllib.parse import urlparse

            parsed = urlparse(dsn)
            return await aiomysql.create_pool(
                host=parsed.hostname or "localhost",
                port=parsed.port or 3306,
                user=parsed.username or "root",
                password=parsed.password or "",
                db=parsed.path.lstrip("/"),
                minsize=1,
                maxsize=5,
                autocommit=True,
            )

        raise ValueError(f"Unsupported DB type: {db_type}")

    def get(self, conn_id: str) -> Connection:
        """Retrieve a connection by ID."""
        conn = self._connections.get(conn_id)
        if conn is None:
            raise KeyError(f"No connection with ID {conn_id}. Connect first.")
        return conn

    async def fetch(
        self, conn_id: str, sql: str, params: tuple | None = None
    ) -> list[dict[str, Any]]:
        """Execute a query and return rows as list of dicts."""
        conn = self.get(conn_id)

        if conn.db_type == DBType.POSTGRES:
            async with conn.pool.acquire() as pg:
                rows = await pg.fetch(sql, *(params or ()))
                return [dict(r) for r in rows]

        if conn.db_type == DBType.SQLITE:
            cursor = await conn.pool.execute(sql, params or ())
            rows = await cursor.fetchall()
            cols = [d[0] for d in cursor.description] if cursor.description else []
            return [dict(zip(cols, row)) for row in rows]

        if conn.db_type == DBType.MYSQL:
            async with conn.pool.acquire() as raw_conn:
                async with raw_conn.cursor(aiomysql.DictCursor) as cur:
                    await cur.execute(sql, params or ())
                    return [dict(r) for r in await cur.fetchall()]

        raise ValueError(f"Unsupported DB type: {conn.db_type}")

    async def execute(
        self, conn_id: str, sql: str, params: tuple | None = None
    ) -> int:
        """Execute a write statement. Returns affected row count."""
        conn = self.get(conn_id)

        if conn.db_type == DBType.POSTGRES:
            async with conn.pool.acquire() as pg:
                result = await pg.execute(sql, *(params or ()))
                # asyncpg returns status string like 'INSERT 0 1'
                parts = result.split() if result else []
                return int(parts[-1]) if parts and parts[-1].isdigit() else 0

        if conn.db_type == DBType.SQLITE:
            cursor = await conn.pool.execute(sql, params or ())
            await conn.pool.commit()
            return cursor.rowcount

        if conn.db_type == DBType.MYSQL:
            import aiomysql

            async with conn.pool.acquire() as raw_conn:
                async with raw_conn.cursor() as cur:
                    await cur.execute(sql, params or ())
                    return cur.rowcount

        raise ValueError(f"Unsupported DB type: {conn.db_type}")

    async def health_check(self, conn_id: str) -> bool:
        """Check if a connection is alive."""
        conn = self.get(conn_id)
        try:
            if conn.db_type == DBType.POSTGRES:
                async with conn.pool.acquire() as pg:
                    await pg.fetchval("SELECT 1")
            elif conn.db_type == DBType.SQLITE:
                await conn.pool.execute("SELECT 1")
            elif conn.db_type == DBType.MYSQL:
                async with conn.pool.acquire() as raw_conn:
                    async with raw_conn.cursor() as cur:
                        await cur.execute("SELECT 1")
            return True
        except Exception:
            return False

    async def close(self, conn_id: str) -> None:
        """Close a connection and remove it."""
        conn = self._connections.pop(conn_id, None)
        if conn is None:
            return
        try:
            if conn.db_type == DBType.POSTGRES:
                await conn.pool.close()
            elif conn.db_type == DBType.SQLITE:
                await conn.pool.close()
            elif conn.db_type == DBType.MYSQL:
                conn.pool.close()
                await conn.pool.wait_closed()
        except Exception:
            pass

    async def close_all(self) -> None:
        """Close all connections."""
        for conn_id in list(self._connections):
            await self.close(conn_id)
