"""Unit tests for berth.safety — no database required."""

import time

import pytest

from berth.safety import (
    Mode,
    check_write_allowed,
    detect_sql_type,
    generate_confirmation_token,
    get_mode,
    is_destructive,
    reset,
    set_mode,
    validate_confirmation_token,
)


@pytest.fixture(autouse=True)
def _clean_state():
    """Reset safety state before each test."""
    reset()
    yield
    reset()


# ── Mode switching ──────────────────────────────────────────────────────

class TestModes:
    def test_default_is_read_only(self):
        assert get_mode() == Mode.READ_ONLY

    def test_set_write(self):
        set_mode("write")
        assert get_mode() == Mode.WRITE

    def test_set_admin(self):
        set_mode("admin")
        assert get_mode() == Mode.ADMIN

    def test_set_invalid_mode(self):
        with pytest.raises(ValueError):
            set_mode("superuser")


# ── SQL type detection ──────────────────────────────────────────────────

class TestSQLDetection:
    @pytest.mark.parametrize(
        "sql, expected",
        [
            ("SELECT * FROM users", "SELECT"),
            ("select id from orders", "SELECT"),
            ("INSERT INTO users (name) VALUES ('a')", "INSERT"),
            ("UPDATE users SET name='b' WHERE id=1", "UPDATE"),
            ("DELETE FROM users WHERE id=1", "DELETE"),
            ("DROP TABLE users", "DROP"),
            ("TRUNCATE TABLE orders", "TRUNCATE"),
            ("ALTER TABLE users ADD COLUMN age INT", "ALTER"),
            ("CREATE TABLE test (id INT)", "CREATE"),
            ("EXPLAIN ANALYZE SELECT 1", "EXPLAIN"),
        ],
    )
    def test_detect_types(self, sql, expected):
        assert detect_sql_type(sql) == expected

    def test_unknown_statement(self):
        assert detect_sql_type("VACUUM") == "UNKNOWN"


# ── Destructive detection ──────────────────────────────────────────────

class TestDestructive:
    def test_drop_is_destructive(self):
        assert is_destructive("DROP TABLE users")

    def test_truncate_is_destructive(self):
        assert is_destructive("TRUNCATE TABLE users")

    def test_delete_without_where_is_destructive(self):
        assert is_destructive("DELETE FROM users")

    def test_delete_without_where_semicolon(self):
        assert is_destructive("DELETE FROM users;")

    def test_delete_with_where_is_not_destructive(self):
        assert not is_destructive("DELETE FROM users WHERE id = 1")

    def test_alter_drop_is_destructive(self):
        assert is_destructive("ALTER TABLE users DROP COLUMN age")

    def test_insert_is_not_destructive(self):
        assert not is_destructive("INSERT INTO users (name) VALUES ('a')")

    def test_select_is_not_destructive(self):
        assert not is_destructive("SELECT * FROM users")


# ── Write permission checks ────────────────────────────────────────────

class TestWritePermissions:
    def test_readonly_allows_select(self):
        allowed, _ = check_write_allowed("SELECT * FROM users")
        assert allowed

    def test_readonly_rejects_insert(self):
        allowed, reason = check_write_allowed("INSERT INTO users (name) VALUES ('a')")
        assert not allowed
        assert "read-only" in reason

    def test_readonly_rejects_update(self):
        allowed, _ = check_write_allowed("UPDATE users SET name='b'")
        assert not allowed

    def test_readonly_rejects_delete(self):
        allowed, _ = check_write_allowed("DELETE FROM users WHERE id=1")
        assert not allowed

    def test_readonly_rejects_drop(self):
        allowed, _ = check_write_allowed("DROP TABLE users")
        assert not allowed

    def test_write_allows_insert(self):
        set_mode("write")
        allowed, _ = check_write_allowed("INSERT INTO users (name) VALUES ('a')")
        assert allowed

    def test_write_allows_update(self):
        set_mode("write")
        allowed, _ = check_write_allowed("UPDATE users SET name='b' WHERE id=1")
        assert allowed

    def test_write_allows_delete_with_where(self):
        set_mode("write")
        allowed, _ = check_write_allowed("DELETE FROM users WHERE id=1")
        assert allowed

    def test_write_rejects_drop(self):
        set_mode("write")
        allowed, reason = check_write_allowed("DROP TABLE users")
        assert not allowed
        assert "admin" in reason.lower()

    def test_write_rejects_truncate(self):
        set_mode("write")
        allowed, _ = check_write_allowed("TRUNCATE TABLE users")
        assert not allowed

    def test_admin_allows_drop(self):
        set_mode("admin")
        allowed, _ = check_write_allowed("DROP TABLE users")
        assert allowed

    def test_admin_allows_truncate(self):
        set_mode("admin")
        allowed, _ = check_write_allowed("TRUNCATE TABLE users")
        assert allowed


# ── Confirmation tokens ────────────────────────────────────────────────

class TestConfirmationTokens:
    def test_generate_and_validate(self):
        token = generate_confirmation_token()
        assert isinstance(token, str)
        assert len(token) == 32
        assert validate_confirmation_token(token)

    def test_token_consumed_on_use(self):
        token = generate_confirmation_token()
        assert validate_confirmation_token(token)
        assert not validate_confirmation_token(token)  # second use fails

    def test_invalid_token_rejected(self):
        assert not validate_confirmation_token("not-a-real-token")

    def test_expired_token_rejected(self):
        import berth.safety as safety_mod

        original_ttl = safety_mod.TOKEN_TTL_SECONDS
        safety_mod.TOKEN_TTL_SECONDS = 0  # expire immediately
        try:
            token = generate_confirmation_token()
            time.sleep(0.05)
            assert not validate_confirmation_token(token)
        finally:
            safety_mod.TOKEN_TTL_SECONDS = original_ttl
