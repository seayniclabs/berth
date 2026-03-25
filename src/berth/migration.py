"""Schema migration generation for Berth.

Compares two database schemas and generates dialect-specific ALTER statements
to migrate from source to target. Supports PostgreSQL, SQLite, and MySQL.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class Dialect(str, Enum):
    POSTGRES = "postgres"
    SQLITE = "sqlite"
    MYSQL = "mysql"


@dataclass
class Column:
    """Represents a single table column."""

    name: str
    data_type: str
    nullable: bool = True
    default: str | None = None

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Column):
            return NotImplemented
        return (
            self.name == other.name
            and self._normalize_type(self.data_type)
                == self._normalize_type(other.data_type)
            and self.nullable == other.nullable
            and self._normalize_default(self.default)
                == self._normalize_default(other.default)
        )

    @staticmethod
    def _normalize_type(t: str) -> str:
        return t.strip().upper()

    @staticmethod
    def _normalize_default(d: str | None) -> str | None:
        if d is None:
            return None
        return d.strip()


@dataclass
class Index:
    """Represents a database index."""

    name: str
    table: str
    columns: list[str]
    unique: bool = False


@dataclass
class ForeignKey:
    """Represents a foreign key constraint."""

    name: str
    table: str
    columns: list[str]
    ref_table: str
    ref_columns: list[str]


@dataclass
class Table:
    """Represents a database table schema."""

    name: str
    columns: list[Column] = field(default_factory=list)
    indexes: list[Index] = field(default_factory=list)
    foreign_keys: list[ForeignKey] = field(default_factory=list)

    def column_map(self) -> dict[str, Column]:
        return {c.name: c for c in self.columns}

    def index_map(self) -> dict[str, Index]:
        return {i.name: i for i in self.indexes}

    def fk_map(self) -> dict[str, ForeignKey]:
        return {f.name: f for f in self.foreign_keys}


@dataclass
class SchemaSnapshot:
    """Complete database schema snapshot."""

    tables: list[Table] = field(default_factory=list)

    def table_map(self) -> dict[str, Table]:
        return {t.name: t for t in self.tables}


# ---------------------------------------------------------------------------
# Schema introspection (from live connections)
# ---------------------------------------------------------------------------


async def introspect_schema(
    mgr: Any, connection_id: str, db_type_str: str
) -> SchemaSnapshot:
    """Pull a full schema snapshot from a live database connection.

    Args:
        mgr: ConnectionManager instance
        connection_id: active connection ID
        db_type_str: one of "postgres", "sqlite", "mysql"
    """
    dialect = Dialect(db_type_str)
    snapshot = SchemaSnapshot()

    if dialect == Dialect.POSTGRES:
        snapshot = await _introspect_postgres(mgr, connection_id)
    elif dialect == Dialect.SQLITE:
        snapshot = await _introspect_sqlite(mgr, connection_id)
    elif dialect == Dialect.MYSQL:
        snapshot = await _introspect_mysql(mgr, connection_id)

    return snapshot


async def _introspect_postgres(mgr: Any, cid: str) -> SchemaSnapshot:
    tables_raw = await mgr.fetch(
        cid,
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'public' AND table_type = 'BASE TABLE' "
        "ORDER BY table_name",
    )
    snapshot = SchemaSnapshot()

    for t in tables_raw:
        tname = t["table_name"]
        cols_raw = await mgr.fetch(
            cid,
            "SELECT column_name, data_type, is_nullable, column_default "
            "FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = $1 "
            "ORDER BY ordinal_position",
            (tname,),
        )
        columns = [
            Column(
                name=c["column_name"],
                data_type=c["data_type"],
                nullable=c["is_nullable"] == "YES",
                default=c["column_default"],
            )
            for c in cols_raw
        ]

        idx_raw = await mgr.fetch(
            cid,
            "SELECT indexname, indexdef FROM pg_indexes "
            "WHERE schemaname = 'public' AND tablename = $1",
            (tname,),
        )
        indexes = []
        for idx in idx_raw:
            idx_cols = _parse_index_columns(idx.get("indexdef", ""))
            unique = "UNIQUE" in (idx.get("indexdef", "")).upper()
            indexes.append(
                Index(
                    name=idx["indexname"],
                    table=tname,
                    columns=idx_cols,
                    unique=unique,
                )
            )

        fk_raw = await mgr.fetch(
            cid,
            "SELECT "
            "  tc.constraint_name, "
            "  kcu.column_name, "
            "  ccu.table_name AS ref_table, "
            "  ccu.column_name AS ref_column "
            "FROM information_schema.table_constraints tc "
            "JOIN information_schema.key_column_usage kcu "
            "  ON tc.constraint_name = kcu.constraint_name "
            "JOIN information_schema.constraint_column_usage ccu "
            "  ON tc.constraint_name = ccu.constraint_name "
            "WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name = $1",
            (tname,),
        )
        fks: dict[str, ForeignKey] = {}
        for fk in fk_raw:
            name = fk["constraint_name"]
            if name not in fks:
                fks[name] = ForeignKey(
                    name=name,
                    table=tname,
                    columns=[],
                    ref_table=fk["ref_table"],
                    ref_columns=[],
                )
            fks[name].columns.append(fk["column_name"])
            fks[name].ref_columns.append(fk["ref_column"])

        snapshot.tables.append(
            Table(
                name=tname,
                columns=columns,
                indexes=indexes,
                foreign_keys=list(fks.values()),
            )
        )

    return snapshot


async def _introspect_sqlite(mgr: Any, cid: str) -> SchemaSnapshot:
    tables_raw = await mgr.fetch(
        cid,
        "SELECT name FROM sqlite_master "
        "WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )
    snapshot = SchemaSnapshot()

    for t in tables_raw:
        tname = t["name"]
        cols_raw = await mgr.fetch(cid, f"PRAGMA table_info('{tname}')")
        columns = [
            Column(
                name=c["name"],
                data_type=c["type"],
                nullable=not bool(c["notnull"]),
                default=c["dflt_value"],
            )
            for c in cols_raw
        ]

        idx_raw = await mgr.fetch(cid, f"PRAGMA index_list('{tname}')")
        indexes = []
        for idx in idx_raw:
            idx_name = idx["name"]
            idx_info = await mgr.fetch(cid, f"PRAGMA index_info('{idx_name}')")
            idx_cols = [i["name"] for i in idx_info]
            indexes.append(
                Index(
                    name=idx_name,
                    table=tname,
                    columns=idx_cols,
                    unique=bool(idx["unique"]),
                )
            )

        fk_raw = await mgr.fetch(cid, f"PRAGMA foreign_key_list('{tname}')")
        fks: dict[int, ForeignKey] = {}
        for fk in fk_raw:
            fk_id = fk["id"]
            if fk_id not in fks:
                fks[fk_id] = ForeignKey(
                    name=f"fk_{tname}_{fk_id}",
                    table=tname,
                    columns=[],
                    ref_table=fk["table"],
                    ref_columns=[],
                )
            fks[fk_id].columns.append(fk["from"])
            fks[fk_id].ref_columns.append(fk["to"])

        snapshot.tables.append(
            Table(
                name=tname,
                columns=columns,
                indexes=indexes,
                foreign_keys=list(fks.values()),
            )
        )

    return snapshot


async def _introspect_mysql(mgr: Any, cid: str) -> SchemaSnapshot:
    tables_raw = await mgr.fetch(
        cid,
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE' "
        "ORDER BY table_name",
    )
    snapshot = SchemaSnapshot()

    for t in tables_raw:
        col = "table_name" if "table_name" in t else "TABLE_NAME"
        tname = t[col]
        cols_raw = await mgr.fetch(
            cid,
            "SELECT column_name, data_type, is_nullable, column_default "
            "FROM information_schema.columns "
            "WHERE table_schema = DATABASE() AND table_name = %s "
            "ORDER BY ordinal_position",
            (tname,),
        )
        columns = [
            Column(
                name=c.get("column_name", c.get("COLUMN_NAME")),
                data_type=c.get("data_type", c.get("DATA_TYPE")),
                nullable=c.get("is_nullable", c.get("IS_NULLABLE")) == "YES",
                default=c.get("column_default", c.get("COLUMN_DEFAULT")),
            )
            for c in cols_raw
        ]

        idx_raw = await mgr.fetch(
            cid,
            "SELECT index_name, column_name, non_unique "
            "FROM information_schema.statistics "
            "WHERE table_schema = DATABASE() AND table_name = %s "
            "ORDER BY index_name, seq_in_index",
            (tname,),
        )
        idx_groups: dict[str, Index] = {}
        for idx in idx_raw:
            iname = idx.get("index_name", idx.get("INDEX_NAME"))
            icol = idx.get("column_name", idx.get("COLUMN_NAME"))
            non_unique = idx.get("non_unique", idx.get("NON_UNIQUE"))
            if iname not in idx_groups:
                idx_groups[iname] = Index(
                    name=iname,
                    table=tname,
                    columns=[],
                    unique=not bool(non_unique),
                )
            idx_groups[iname].columns.append(icol)
        indexes = list(idx_groups.values())

        fk_raw = await mgr.fetch(
            cid,
            "SELECT constraint_name, column_name, "
            "  referenced_table_name, referenced_column_name "
            "FROM information_schema.key_column_usage "
            "WHERE table_schema = DATABASE() AND table_name = %s "
            "  AND referenced_table_name IS NOT NULL "
            "ORDER BY constraint_name, ordinal_position",
            (tname,),
        )
        fks: dict[str, ForeignKey] = {}
        for fk in fk_raw:
            cname = fk.get("constraint_name", fk.get("CONSTRAINT_NAME"))
            if cname not in fks:
                fks[cname] = ForeignKey(
                    name=cname,
                    table=tname,
                    columns=[],
                    ref_table=fk.get(
                        "referenced_table_name",
                        fk.get("REFERENCED_TABLE_NAME"),
                    ),
                    ref_columns=[],
                )
            fks[cname].columns.append(
                fk.get("column_name", fk.get("COLUMN_NAME"))
            )
            fks[cname].ref_columns.append(
                fk.get("referenced_column_name", fk.get("REFERENCED_COLUMN_NAME"))
            )

        snapshot.tables.append(
            Table(
                name=tname,
                columns=columns,
                indexes=indexes,
                foreign_keys=list(fks.values()),
            )
        )

    return snapshot


# ---------------------------------------------------------------------------
# SQL parsing — build schema from CREATE TABLE statements
# ---------------------------------------------------------------------------


def parse_create_statements(sql: str) -> SchemaSnapshot:
    """Parse CREATE TABLE / CREATE INDEX SQL into a SchemaSnapshot.

    Handles the subset of DDL syntax common across PostgreSQL, SQLite, and MySQL.
    This is intentionally forgiving — it extracts what it can.
    """
    snapshot = SchemaSnapshot()
    tables: dict[str, Table] = {}

    # Find CREATE TABLE blocks
    for match in re.finditer(
        r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
        r"[`\"']?(\w+)[`\"']?\s*\((.*?)\)\s*;",
        sql,
        re.IGNORECASE | re.DOTALL,
    ):
        tname = match.group(1)
        body = match.group(2)
        table = _parse_table_body(tname, body)
        tables[tname] = table

    # Find standalone CREATE INDEX statements
    for match in re.finditer(
        r"CREATE\s+(UNIQUE\s+)?INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?"
        r"[`\"']?(\w+)[`\"']?\s+ON\s+[`\"']?(\w+)[`\"']?\s*"
        r"\(([^)]+)\)\s*;",
        sql,
        re.IGNORECASE,
    ):
        unique = bool(match.group(1))
        idx_name = match.group(2)
        tbl_name = match.group(3)
        cols = [c.strip().strip("`\"'") for c in match.group(4).split(",")]
        idx = Index(name=idx_name, table=tbl_name, columns=cols, unique=unique)
        if tbl_name in tables:
            tables[tbl_name].indexes.append(idx)

    snapshot.tables = list(tables.values())
    return snapshot


def _parse_table_body(table_name: str, body: str) -> Table:
    """Parse the inner body of a CREATE TABLE statement."""
    columns: list[Column] = []
    indexes: list[Index] = []
    foreign_keys: list[ForeignKey] = []

    # Split on commas, but respect parentheses (for CHECK, FK references, etc.)
    parts = _split_respecting_parens(body)

    fk_counter = 0
    for part in parts:
        part = part.strip()
        upper = part.upper()

        # Skip table-level constraints that aren't FK or INDEX
        if upper.startswith("PRIMARY KEY"):
            continue
        if upper.startswith("CHECK"):
            continue
        if upper.startswith("UNIQUE"):
            # Could be a UNIQUE constraint on columns
            cols_match = re.search(r"\(([^)]+)\)", part)
            if cols_match:
                cols = [
                    c.strip().strip("`\"'")
                    for c in cols_match.group(1).split(",")
                ]
                indexes.append(
                    Index(
                        name=f"uq_{table_name}_{'_'.join(cols)}",
                        table=table_name,
                        columns=cols,
                        unique=True,
                    )
                )
            continue
        if upper.startswith("CONSTRAINT"):
            # Named constraint — check if FK
            fk_match = re.match(
                r"CONSTRAINT\s+[`\"']?(\w+)[`\"']?\s+FOREIGN\s+KEY\s*"
                r"\(([^)]+)\)\s*REFERENCES\s+[`\"']?(\w+)[`\"']?\s*"
                r"\(([^)]+)\)",
                part,
                re.IGNORECASE,
            )
            if fk_match:
                fk_name = fk_match.group(1)
                fk_cols = [
                    c.strip().strip("`\"'")
                    for c in fk_match.group(2).split(",")
                ]
                ref_table = fk_match.group(3)
                ref_cols = [
                    c.strip().strip("`\"'")
                    for c in fk_match.group(4).split(",")
                ]
                foreign_keys.append(
                    ForeignKey(
                        name=fk_name,
                        table=table_name,
                        columns=fk_cols,
                        ref_table=ref_table,
                        ref_columns=ref_cols,
                    )
                )
            continue
        if upper.startswith("FOREIGN KEY"):
            fk_match = re.match(
                r"FOREIGN\s+KEY\s*\(([^)]+)\)\s*REFERENCES\s+"
                r"[`\"']?(\w+)[`\"']?\s*\(([^)]+)\)",
                part,
                re.IGNORECASE,
            )
            if fk_match:
                fk_cols = [
                    c.strip().strip("`\"'")
                    for c in fk_match.group(1).split(",")
                ]
                ref_table = fk_match.group(2)
                ref_cols = [
                    c.strip().strip("`\"'")
                    for c in fk_match.group(3).split(",")
                ]
                foreign_keys.append(
                    ForeignKey(
                        name=f"fk_{table_name}_{fk_counter}",
                        table=table_name,
                        columns=fk_cols,
                        ref_table=ref_table,
                        ref_columns=ref_cols,
                    )
                )
                fk_counter += 1
            continue

        # Column definition
        col = _parse_column(part)
        if col:
            columns.append(col)

    return Table(
        name=table_name,
        columns=columns,
        indexes=indexes,
        foreign_keys=foreign_keys,
    )


def _parse_column(definition: str) -> Column | None:
    """Parse a single column definition like 'name VARCHAR(255) NOT NULL DEFAULT 'foo''."""
    # Match: column_name TYPE ...
    match = re.match(
        r"[`\"']?(\w+)[`\"']?\s+(\w+(?:\s*\([^)]*\))?)",
        definition.strip(),
        re.IGNORECASE,
    )
    if not match:
        return None

    name = match.group(1)
    data_type = match.group(2).strip()
    rest = definition[match.end():].strip().upper()

    nullable = "NOT NULL" not in rest
    default = None
    default_match = re.search(
        r"DEFAULT\s+(.+?)(?:\s+NOT\s+NULL|\s+NULL|\s+PRIMARY|\s+REFERENCES|\s+CHECK|\s*$)",
        definition[match.end():].strip(),
        re.IGNORECASE,
    )
    if default_match:
        default = default_match.group(1).strip().rstrip(",")

    return Column(name=name, data_type=data_type, nullable=nullable, default=default)


def _split_respecting_parens(s: str) -> list[str]:
    """Split a string on commas, but respect parenthesized groups."""
    parts: list[str] = []
    depth = 0
    current: list[str] = []

    for char in s:
        if char == "(":
            depth += 1
            current.append(char)
        elif char == ")":
            depth -= 1
            current.append(char)
        elif char == "," and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(char)

    if current:
        parts.append("".join(current))

    return parts


def _parse_index_columns(indexdef: str) -> list[str]:
    """Extract column names from a PostgreSQL index definition string."""
    match = re.search(r"\(([^)]+)\)", indexdef)
    if not match:
        return []
    return [c.strip().strip("`\"'") for c in match.group(1).split(",")]


# ---------------------------------------------------------------------------
# Diff engine — compare two snapshots
# ---------------------------------------------------------------------------


@dataclass
class SchemaDiff:
    """Differences between two schema snapshots."""

    new_tables: list[Table] = field(default_factory=list)
    dropped_tables: list[str] = field(default_factory=list)
    added_columns: list[tuple[str, Column]] = field(default_factory=list)  # (table, col)
    dropped_columns: list[tuple[str, str]] = field(default_factory=list)  # (table, col_name)
    altered_columns: list[tuple[str, Column, Column]] = field(
        default_factory=list
    )  # (table, old, new)
    new_indexes: list[Index] = field(default_factory=list)
    dropped_indexes: list[tuple[str, str]] = field(default_factory=list)  # (table, idx_name)
    new_foreign_keys: list[ForeignKey] = field(default_factory=list)
    dropped_foreign_keys: list[tuple[str, str]] = field(
        default_factory=list
    )  # (table, fk_name)


def diff_schemas(source: SchemaSnapshot, target: SchemaSnapshot) -> SchemaDiff:
    """Compare source (current) schema to target (desired) schema.

    Returns the operations needed to transform source into target.
    """
    result = SchemaDiff()
    src_map = source.table_map()
    tgt_map = target.table_map()

    # New tables (in target but not source)
    for tname, table in tgt_map.items():
        if tname not in src_map:
            result.new_tables.append(table)

    # Dropped tables (in source but not target)
    for tname in src_map:
        if tname not in tgt_map:
            result.dropped_tables.append(tname)

    # Modified tables (in both)
    for tname in src_map:
        if tname not in tgt_map:
            continue
        src_tbl = src_map[tname]
        tgt_tbl = tgt_map[tname]

        _diff_columns(tname, src_tbl, tgt_tbl, result)
        _diff_indexes(tname, src_tbl, tgt_tbl, result)
        _diff_foreign_keys(tname, src_tbl, tgt_tbl, result)

    return result


def _diff_columns(
    table: str, src: Table, tgt: Table, diff: SchemaDiff
) -> None:
    src_cols = src.column_map()
    tgt_cols = tgt.column_map()

    for cname, col in tgt_cols.items():
        if cname not in src_cols:
            diff.added_columns.append((table, col))
        elif src_cols[cname] != col:
            diff.altered_columns.append((table, src_cols[cname], col))

    for cname in src_cols:
        if cname not in tgt_cols:
            diff.dropped_columns.append((table, cname))


def _diff_indexes(
    table: str, src: Table, tgt: Table, diff: SchemaDiff
) -> None:
    src_idxs = src.index_map()
    tgt_idxs = tgt.index_map()

    for iname, idx in tgt_idxs.items():
        if iname not in src_idxs:
            diff.new_indexes.append(idx)

    for iname in src_idxs:
        if iname not in tgt_idxs:
            diff.dropped_indexes.append((table, iname))


def _diff_foreign_keys(
    table: str, src: Table, tgt: Table, diff: SchemaDiff
) -> None:
    src_fks = src.fk_map()
    tgt_fks = tgt.fk_map()

    for fname, fk in tgt_fks.items():
        if fname not in src_fks:
            diff.new_foreign_keys.append(fk)

    for fname in src_fks:
        if fname not in tgt_fks:
            diff.dropped_foreign_keys.append((table, fname))


# ---------------------------------------------------------------------------
# SQL generation — dialect-aware migration output
# ---------------------------------------------------------------------------


def generate_migration_sql(diff: SchemaDiff, dialect: Dialect) -> str:
    """Generate migration SQL from a SchemaDiff.

    Produces copy-paste-ready SQL with safety comments.
    """
    lines: list[str] = []
    lines.append("-- Migration generated by Berth")
    lines.append(f"-- Dialect: {dialect.value}")
    lines.append("-- Review carefully before executing.\n")

    if not _has_changes(diff):
        lines.append("-- No schema differences detected.")
        return "\n".join(lines)

    # New tables
    for table in diff.new_tables:
        lines.append(f"-- Create new table: {table.name}")
        lines.append(_gen_create_table(table, dialect))
        lines.append("")

    # Dropped tables (commented out for safety)
    for tname in diff.dropped_tables:
        lines.append(f"-- WARNING: Table '{tname}' exists in source but not in target.")
        lines.append(f"-- Uncomment the following line to drop it. THIS IS DESTRUCTIVE.")
        lines.append(f"-- DROP TABLE {_quote(tname, dialect)};")
        lines.append("")

    # Added columns
    for tname, col in diff.added_columns:
        lines.append(f"-- Add column '{col.name}' to '{tname}'")
        lines.append(_gen_add_column(tname, col, dialect))
        lines.append("")

    # Dropped columns
    for tname, cname in diff.dropped_columns:
        lines.append(f"-- WARNING: Column '{cname}' exists in '{tname}' but not in target.")
        lines.append(f"-- Uncomment the following line to drop it. THIS IS DESTRUCTIVE.")
        if dialect == Dialect.SQLITE:
            lines.append(
                f"-- SQLite does not support DROP COLUMN in older versions."
            )
            lines.append(
                f"-- For SQLite < 3.35.0, use the table rebuild pattern below."
            )
        lines.append(f"-- ALTER TABLE {_quote(tname, dialect)} DROP COLUMN {_quote(cname, dialect)};")
        lines.append("")

    # Altered columns
    for tname, old_col, new_col in diff.altered_columns:
        lines.append(
            f"-- Alter column '{old_col.name}' in '{tname}': "
            f"{_describe_column_change(old_col, new_col)}"
        )
        alter_sql = _gen_alter_column(tname, old_col, new_col, dialect)
        for stmt in alter_sql:
            lines.append(stmt)
        lines.append("")

    # New indexes
    for idx in diff.new_indexes:
        lines.append(f"-- Create index '{idx.name}' on '{idx.table}'")
        unique = "UNIQUE " if idx.unique else ""
        cols = ", ".join(_quote(c, dialect) for c in idx.columns)
        lines.append(
            f"CREATE {unique}INDEX {_quote(idx.name, dialect)} "
            f"ON {_quote(idx.table, dialect)} ({cols});"
        )
        lines.append("")

    # Dropped indexes
    for tname, iname in diff.dropped_indexes:
        lines.append(f"-- Drop index '{iname}' (was on '{tname}')")
        if dialect == Dialect.MYSQL:
            lines.append(f"DROP INDEX {_quote(iname, dialect)} ON {_quote(tname, dialect)};")
        else:
            lines.append(f"DROP INDEX {_quote(iname, dialect)};")
        lines.append("")

    # New foreign keys
    for fk in diff.new_foreign_keys:
        lines.append(
            f"-- Add foreign key '{fk.name}' on '{fk.table}' "
            f"-> '{fk.ref_table}'"
        )
        if dialect == Dialect.SQLITE:
            lines.append(
                f"-- SQLite does not support ADD CONSTRAINT. "
                f"Use the table rebuild pattern instead."
            )
        else:
            cols = ", ".join(_quote(c, dialect) for c in fk.columns)
            ref_cols = ", ".join(_quote(c, dialect) for c in fk.ref_columns)
            lines.append(
                f"ALTER TABLE {_quote(fk.table, dialect)} "
                f"ADD CONSTRAINT {_quote(fk.name, dialect)} "
                f"FOREIGN KEY ({cols}) REFERENCES {_quote(fk.ref_table, dialect)} ({ref_cols});"
            )
        lines.append("")

    # Dropped foreign keys
    for tname, fname in diff.dropped_foreign_keys:
        lines.append(f"-- Drop foreign key '{fname}' from '{tname}'")
        if dialect == Dialect.SQLITE:
            lines.append(
                f"-- SQLite does not support DROP CONSTRAINT. "
                f"Use the table rebuild pattern instead."
            )
        elif dialect == Dialect.MYSQL:
            lines.append(
                f"ALTER TABLE {_quote(tname, dialect)} DROP FOREIGN KEY {_quote(fname, dialect)};"
            )
        else:
            lines.append(
                f"ALTER TABLE {_quote(tname, dialect)} DROP CONSTRAINT {_quote(fname, dialect)};"
            )
        lines.append("")

    # SQLite table rebuild helper (if needed)
    if dialect == Dialect.SQLITE and _needs_sqlite_rebuild(diff):
        lines.append(_gen_sqlite_rebuild_comment())

    return "\n".join(lines)


def _has_changes(diff: SchemaDiff) -> bool:
    return bool(
        diff.new_tables
        or diff.dropped_tables
        or diff.added_columns
        or diff.dropped_columns
        or diff.altered_columns
        or diff.new_indexes
        or diff.dropped_indexes
        or diff.new_foreign_keys
        or diff.dropped_foreign_keys
    )


def _quote(name: str, dialect: Dialect) -> str:
    """Quote an identifier for the given dialect."""
    if dialect == Dialect.MYSQL:
        return f"`{name}`"
    return f'"{name}"'


def _col_type_sql(col: Column, dialect: Dialect) -> str:
    """Generate full column type with nullable and default."""
    parts = [col.data_type]
    if not col.nullable:
        parts.append("NOT NULL")
    if col.default is not None:
        parts.append(f"DEFAULT {col.default}")
    return " ".join(parts)


def _gen_create_table(table: Table, dialect: Dialect) -> str:
    """Generate a CREATE TABLE statement."""
    lines: list[str] = []
    lines.append(f"CREATE TABLE {_quote(table.name, dialect)} (")

    col_defs = []
    for col in table.columns:
        col_defs.append(
            f"    {_quote(col.name, dialect)} {_col_type_sql(col, dialect)}"
        )

    for fk in table.foreign_keys:
        cols = ", ".join(_quote(c, dialect) for c in fk.columns)
        ref_cols = ", ".join(_quote(c, dialect) for c in fk.ref_columns)
        if dialect == Dialect.SQLITE:
            col_defs.append(
                f"    FOREIGN KEY ({cols}) REFERENCES "
                f"{_quote(fk.ref_table, dialect)} ({ref_cols})"
            )
        else:
            col_defs.append(
                f"    CONSTRAINT {_quote(fk.name, dialect)} "
                f"FOREIGN KEY ({cols}) REFERENCES "
                f"{_quote(fk.ref_table, dialect)} ({ref_cols})"
            )

    lines.append(",\n".join(col_defs))
    lines.append(");")
    return "\n".join(lines)


def _gen_add_column(table: str, col: Column, dialect: Dialect) -> str:
    return (
        f"ALTER TABLE {_quote(table, dialect)} "
        f"ADD COLUMN {_quote(col.name, dialect)} {_col_type_sql(col, dialect)};"
    )


def _gen_alter_column(
    table: str, old_col: Column, new_col: Column, dialect: Dialect
) -> list[str]:
    """Generate ALTER COLUMN statements. Returns a list of SQL statements."""
    stmts: list[str] = []
    tq = _quote(table, dialect)
    cq = _quote(new_col.name, dialect)

    if dialect == Dialect.SQLITE:
        stmts.append(
            f"-- SQLite does not support ALTER COLUMN directly."
        )
        stmts.append(
            f"-- To change column '{new_col.name}' in '{table}', "
            f"use the table rebuild pattern (see end of migration)."
        )
        return stmts

    type_changed = (
        Column._normalize_type(old_col.data_type)
        != Column._normalize_type(new_col.data_type)
    )
    nullable_changed = old_col.nullable != new_col.nullable
    default_changed = (
        Column._normalize_default(old_col.default)
        != Column._normalize_default(new_col.default)
    )

    if dialect == Dialect.POSTGRES:
        if type_changed:
            stmts.append(
                f"ALTER TABLE {tq} ALTER COLUMN {cq} "
                f"TYPE {new_col.data_type};"
            )
        if nullable_changed:
            if new_col.nullable:
                stmts.append(
                    f"ALTER TABLE {tq} ALTER COLUMN {cq} DROP NOT NULL;"
                )
            else:
                stmts.append(
                    f"ALTER TABLE {tq} ALTER COLUMN {cq} SET NOT NULL;"
                )
        if default_changed:
            if new_col.default is not None:
                stmts.append(
                    f"ALTER TABLE {tq} ALTER COLUMN {cq} "
                    f"SET DEFAULT {new_col.default};"
                )
            else:
                stmts.append(
                    f"ALTER TABLE {tq} ALTER COLUMN {cq} DROP DEFAULT;"
                )

    elif dialect == Dialect.MYSQL:
        # MySQL uses MODIFY COLUMN for all column changes
        stmts.append(
            f"ALTER TABLE {tq} MODIFY COLUMN {cq} "
            f"{_col_type_sql(new_col, dialect)};"
        )

    return stmts


def _describe_column_change(old: Column, new: Column) -> str:
    """Human-readable description of what changed in a column."""
    changes = []
    if Column._normalize_type(old.data_type) != Column._normalize_type(new.data_type):
        changes.append(f"type {old.data_type} -> {new.data_type}")
    if old.nullable != new.nullable:
        changes.append(
            f"{'nullable' if old.nullable else 'not null'} -> "
            f"{'nullable' if new.nullable else 'not null'}"
        )
    if Column._normalize_default(old.default) != Column._normalize_default(new.default):
        changes.append(f"default {old.default!r} -> {new.default!r}")
    return ", ".join(changes) if changes else "metadata change"


def _needs_sqlite_rebuild(diff: SchemaDiff) -> bool:
    """Check if any changes require the SQLite table rebuild pattern."""
    return bool(
        diff.altered_columns
        or diff.dropped_columns
        or diff.new_foreign_keys
        or diff.dropped_foreign_keys
    )


def _gen_sqlite_rebuild_comment() -> str:
    """Generate the SQLite table rebuild pattern documentation."""
    return """
-- ============================================================================
-- SQLite Table Rebuild Pattern
-- ============================================================================
-- SQLite has limited ALTER TABLE support. To change column types, drop columns
-- (pre-3.35.0), or modify constraints, use this pattern:
--
--   1. Create a new table with the desired schema:
--      CREATE TABLE "tablename_new" ( ... );
--
--   2. Copy data from the old table:
--      INSERT INTO "tablename_new" SELECT col1, col2, ... FROM "tablename";
--
--   3. Drop the old table:
--      DROP TABLE "tablename";
--
--   4. Rename the new table:
--      ALTER TABLE "tablename_new" RENAME TO "tablename";
--
--   5. Recreate any indexes:
--      CREATE INDEX ... ON "tablename" (...);
--
-- Wrap the entire operation in a transaction:
--   BEGIN TRANSACTION;
--   ... (steps 1-5) ...
--   COMMIT;
-- ============================================================================
"""
