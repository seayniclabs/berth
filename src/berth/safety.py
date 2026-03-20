"""Safety mode management for Berth.

Controls what SQL operations are permitted based on the current operating mode.
Provides confirmation tokens for destructive admin operations.
"""

import re
import time
import uuid
from enum import Enum


class Mode(str, Enum):
    READ_ONLY = "read-only"
    WRITE = "write"
    ADMIN = "admin"


# Module-level state
_current_mode: Mode = Mode.READ_ONLY
_pending_tokens: dict[str, float] = {}  # token -> expiry timestamp

TOKEN_TTL_SECONDS = 60

# SQL statement type detection patterns
_SQL_PATTERNS: list[tuple[str, re.Pattern]] = [
    ("SELECT", re.compile(r"^\s*SELECT\b", re.IGNORECASE)),
    ("INSERT", re.compile(r"^\s*INSERT\b", re.IGNORECASE)),
    ("UPDATE", re.compile(r"^\s*UPDATE\b", re.IGNORECASE)),
    ("DELETE", re.compile(r"^\s*DELETE\b", re.IGNORECASE)),
    ("DROP", re.compile(r"^\s*DROP\b", re.IGNORECASE)),
    ("TRUNCATE", re.compile(r"^\s*TRUNCATE\b", re.IGNORECASE)),
    ("ALTER", re.compile(r"^\s*ALTER\b", re.IGNORECASE)),
    ("CREATE", re.compile(r"^\s*CREATE\b", re.IGNORECASE)),
    ("EXPLAIN", re.compile(r"^\s*EXPLAIN\b", re.IGNORECASE)),
]

# Destructive operation patterns
_DESTRUCTIVE_PATTERNS: list[re.Pattern] = [
    re.compile(r"^\s*DROP\b", re.IGNORECASE),
    re.compile(r"^\s*TRUNCATE\b", re.IGNORECASE),
    re.compile(r"^\s*ALTER\b.*\bDROP\b", re.IGNORECASE | re.DOTALL),
    # DELETE without WHERE clause
    re.compile(r"^\s*DELETE\s+FROM\s+\S+\s*;?\s*$", re.IGNORECASE),
]


def set_mode(mode: str) -> Mode:
    """Switch the operating mode. Returns the new mode."""
    global _current_mode
    _current_mode = Mode(mode)
    return _current_mode


def get_mode() -> Mode:
    """Return the current operating mode."""
    return _current_mode


def detect_sql_type(sql: str) -> str:
    """Detect the SQL statement type from the query string."""
    for name, pattern in _SQL_PATTERNS:
        if pattern.search(sql):
            return name
    return "UNKNOWN"


def is_destructive(sql: str) -> bool:
    """Check if a SQL statement is destructive (DROP, TRUNCATE, DELETE without WHERE, ALTER DROP)."""
    return any(p.search(sql) for p in _DESTRUCTIVE_PATTERNS)


def check_write_allowed(sql: str) -> tuple[bool, str]:
    """Check whether the current mode permits the given SQL.

    Returns (allowed, reason). If not allowed, reason explains why.
    """
    sql_type = detect_sql_type(sql)

    # SELECT and EXPLAIN are always allowed
    if sql_type in ("SELECT", "EXPLAIN"):
        return True, "ok"

    # Read-only rejects everything except SELECT/EXPLAIN
    if _current_mode == Mode.READ_ONLY:
        return False, f"{sql_type} blocked: server is in read-only mode"

    # Write mode: allow INSERT/UPDATE/DELETE (non-destructive), block destructive ops
    if _current_mode == Mode.WRITE:
        if is_destructive(sql):
            return False, f"Destructive operation ({sql_type}) blocked: requires admin mode"
        if sql_type in ("INSERT", "UPDATE", "DELETE", "CREATE"):
            return True, "ok"
        if sql_type in ("DROP", "TRUNCATE", "ALTER"):
            return False, f"{sql_type} blocked: requires admin mode"
        return False, f"{sql_type} blocked in write mode"

    # Admin mode: allow everything (destructive ops need confirmation token externally)
    if _current_mode == Mode.ADMIN:
        return True, "ok"

    return False, "Unknown mode"


def generate_confirmation_token() -> str:
    """Generate a UUID confirmation token that expires after TOKEN_TTL_SECONDS."""
    token = uuid.uuid4().hex
    _pending_tokens[token] = time.time() + TOKEN_TTL_SECONDS
    # Prune expired tokens
    now = time.time()
    expired = [t for t, exp in _pending_tokens.items() if exp < now]
    for t in expired:
        del _pending_tokens[t]
    return token


def validate_confirmation_token(token: str) -> bool:
    """Validate a confirmation token. Consumes it on success."""
    expiry = _pending_tokens.pop(token, None)
    if expiry is None:
        return False
    return time.time() < expiry


def reset() -> None:
    """Reset state to defaults. Used in tests."""
    global _current_mode
    _current_mode = Mode.READ_ONLY
    _pending_tokens.clear()
