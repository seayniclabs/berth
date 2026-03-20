"""Berth MCP server test fixtures.

Provides throwaway database connections for integration testing.
Databases are seeded via docker-compose.test.yml volumes.
"""

import os
import subprocess

import pytest

POSTGRES_DSN = os.getenv(
    "BERTH_TEST_POSTGRES",
    "postgresql://testuser:testpass@localhost:15432/testdb",
)
MYSQL_DSN = os.getenv(
    "BERTH_TEST_MYSQL",
    "mysql://testuser:testpass@localhost:13306/testdb",
)
SQLITE_PATH = ":memory:"

COMPOSE_FILE = os.path.join(os.path.dirname(__file__), "docker-compose.test.yml")


@pytest.fixture(scope="session")
def db_targets():
    """Start Postgres + MySQL containers for the test session."""
    if os.getenv("BERTH_SKIP_DOCKER"):
        yield
        return

    subprocess.run(
        ["docker", "compose", "-f", COMPOSE_FILE, "up", "-d", "--wait"],
        check=True,
        capture_output=True,
    )
    yield
    subprocess.run(
        ["docker", "compose", "-f", COMPOSE_FILE, "down", "-v"],
        check=True,
        capture_output=True,
    )


@pytest.fixture
def postgres_dsn(db_targets):
    return POSTGRES_DSN


@pytest.fixture
def mysql_dsn(db_targets):
    return MYSQL_DSN


@pytest.fixture
def sqlite_dsn():
    return SQLITE_PATH
