"""Unit tests for berth.migration — no database required."""

import pytest

from berth.migration import (
    Column,
    Dialect,
    ForeignKey,
    Index,
    SchemaSnapshot,
    SchemaDiff,
    Table,
    diff_schemas,
    generate_migration_sql,
    parse_create_statements,
)


# ── SQL Parsing ────────────────────────────────────────────────────────


class TestParseCreateStatements:
    def test_simple_table(self):
        sql = """
        CREATE TABLE users (
            id INTEGER NOT NULL,
            name VARCHAR(255),
            email TEXT NOT NULL DEFAULT 'none'
        );
        """
        snap = parse_create_statements(sql)
        assert len(snap.tables) == 1
        t = snap.tables[0]
        assert t.name == "users"
        assert len(t.columns) == 3

        assert t.columns[0].name == "id"
        assert t.columns[0].nullable is False

        assert t.columns[1].name == "name"
        assert t.columns[1].nullable is True

        assert t.columns[2].name == "email"
        assert t.columns[2].nullable is False
        assert t.columns[2].default == "'none'"

    def test_multiple_tables(self):
        sql = """
        CREATE TABLE orders (id INTEGER NOT NULL);
        CREATE TABLE items (id INTEGER NOT NULL, order_id INTEGER);
        """
        snap = parse_create_statements(sql)
        assert len(snap.tables) == 2
        names = [t.name for t in snap.tables]
        assert "orders" in names
        assert "items" in names

    def test_foreign_key_inline(self):
        sql = """
        CREATE TABLE items (
            id INTEGER NOT NULL,
            order_id INTEGER,
            FOREIGN KEY (order_id) REFERENCES orders (id)
        );
        """
        snap = parse_create_statements(sql)
        t = snap.tables[0]
        assert len(t.foreign_keys) == 1
        fk = t.foreign_keys[0]
        assert fk.columns == ["order_id"]
        assert fk.ref_table == "orders"
        assert fk.ref_columns == ["id"]

    def test_named_constraint_fk(self):
        sql = """
        CREATE TABLE items (
            id INTEGER NOT NULL,
            order_id INTEGER,
            CONSTRAINT fk_order FOREIGN KEY (order_id) REFERENCES orders (id)
        );
        """
        snap = parse_create_statements(sql)
        fk = snap.tables[0].foreign_keys[0]
        assert fk.name == "fk_order"

    def test_create_index(self):
        sql = """
        CREATE TABLE users (id INTEGER NOT NULL, email TEXT);
        CREATE UNIQUE INDEX idx_users_email ON users (email);
        """
        snap = parse_create_statements(sql)
        t = snap.tables[0]
        assert len(t.indexes) == 1
        idx = t.indexes[0]
        assert idx.name == "idx_users_email"
        assert idx.unique is True
        assert idx.columns == ["email"]

    def test_if_not_exists(self):
        sql = """
        CREATE TABLE IF NOT EXISTS users (id INTEGER NOT NULL);
        CREATE INDEX IF NOT EXISTS idx_id ON users (id);
        """
        snap = parse_create_statements(sql)
        assert len(snap.tables) == 1
        assert snap.tables[0].name == "users"


# ── Schema Diff ────────────────────────────────────────────────────────


class TestDiffSchemas:
    def _make_snapshot(self, tables: list[Table]) -> SchemaSnapshot:
        return SchemaSnapshot(tables=tables)

    def test_no_changes(self):
        t = Table(name="users", columns=[Column("id", "INTEGER")])
        diff = diff_schemas(
            self._make_snapshot([t]), self._make_snapshot([t])
        )
        assert not diff.new_tables
        assert not diff.dropped_tables
        assert not diff.added_columns
        assert not diff.dropped_columns
        assert not diff.altered_columns

    def test_new_table(self):
        src = self._make_snapshot([])
        tgt = self._make_snapshot(
            [Table(name="users", columns=[Column("id", "INTEGER")])]
        )
        diff = diff_schemas(src, tgt)
        assert len(diff.new_tables) == 1
        assert diff.new_tables[0].name == "users"

    def test_dropped_table(self):
        src = self._make_snapshot(
            [Table(name="old_table", columns=[Column("id", "INTEGER")])]
        )
        tgt = self._make_snapshot([])
        diff = diff_schemas(src, tgt)
        assert diff.dropped_tables == ["old_table"]

    def test_added_column(self):
        src = self._make_snapshot(
            [Table(name="users", columns=[Column("id", "INTEGER")])]
        )
        tgt = self._make_snapshot(
            [
                Table(
                    name="users",
                    columns=[
                        Column("id", "INTEGER"),
                        Column("email", "TEXT"),
                    ],
                )
            ]
        )
        diff = diff_schemas(src, tgt)
        assert len(diff.added_columns) == 1
        assert diff.added_columns[0] == ("users", Column("email", "TEXT"))

    def test_dropped_column(self):
        src = self._make_snapshot(
            [
                Table(
                    name="users",
                    columns=[
                        Column("id", "INTEGER"),
                        Column("old_col", "TEXT"),
                    ],
                )
            ]
        )
        tgt = self._make_snapshot(
            [Table(name="users", columns=[Column("id", "INTEGER")])]
        )
        diff = diff_schemas(src, tgt)
        assert diff.dropped_columns == [("users", "old_col")]

    def test_altered_column_type(self):
        src = self._make_snapshot(
            [Table(name="users", columns=[Column("age", "INTEGER")])]
        )
        tgt = self._make_snapshot(
            [Table(name="users", columns=[Column("age", "BIGINT")])]
        )
        diff = diff_schemas(src, tgt)
        assert len(diff.altered_columns) == 1
        table, old, new = diff.altered_columns[0]
        assert table == "users"
        assert old.data_type == "INTEGER"
        assert new.data_type == "BIGINT"

    def test_altered_column_nullability(self):
        src = self._make_snapshot(
            [
                Table(
                    name="users",
                    columns=[Column("name", "TEXT", nullable=True)],
                )
            ]
        )
        tgt = self._make_snapshot(
            [
                Table(
                    name="users",
                    columns=[Column("name", "TEXT", nullable=False)],
                )
            ]
        )
        diff = diff_schemas(src, tgt)
        assert len(diff.altered_columns) == 1

    def test_new_index(self):
        idx = Index(name="idx_email", table="users", columns=["email"])
        src = self._make_snapshot(
            [Table(name="users", columns=[Column("email", "TEXT")])]
        )
        tgt = self._make_snapshot(
            [
                Table(
                    name="users",
                    columns=[Column("email", "TEXT")],
                    indexes=[idx],
                )
            ]
        )
        diff = diff_schemas(src, tgt)
        assert len(diff.new_indexes) == 1

    def test_dropped_index(self):
        idx = Index(name="idx_email", table="users", columns=["email"])
        src = self._make_snapshot(
            [
                Table(
                    name="users",
                    columns=[Column("email", "TEXT")],
                    indexes=[idx],
                )
            ]
        )
        tgt = self._make_snapshot(
            [Table(name="users", columns=[Column("email", "TEXT")])]
        )
        diff = diff_schemas(src, tgt)
        assert diff.dropped_indexes == [("users", "idx_email")]


# ── SQL Generation ─────────────────────────────────────────────────────


class TestGenerateMigrationSQL:
    def test_no_changes(self):
        diff = SchemaDiff()
        sql = generate_migration_sql(diff, Dialect.POSTGRES)
        assert "No schema differences detected" in sql

    def test_create_table_postgres(self):
        diff = SchemaDiff(
            new_tables=[
                Table(
                    name="users",
                    columns=[
                        Column("id", "INTEGER", nullable=False),
                        Column("name", "TEXT"),
                    ],
                )
            ]
        )
        sql = generate_migration_sql(diff, Dialect.POSTGRES)
        assert 'CREATE TABLE "users"' in sql
        assert '"id" INTEGER NOT NULL' in sql
        assert '"name" TEXT' in sql

    def test_create_table_mysql(self):
        diff = SchemaDiff(
            new_tables=[
                Table(
                    name="users",
                    columns=[Column("id", "INTEGER", nullable=False)],
                )
            ]
        )
        sql = generate_migration_sql(diff, Dialect.MYSQL)
        assert "CREATE TABLE `users`" in sql
        assert "`id` INTEGER NOT NULL" in sql

    def test_drop_table_is_commented(self):
        diff = SchemaDiff(dropped_tables=["old_table"])
        sql = generate_migration_sql(diff, Dialect.POSTGRES)
        assert "WARNING" in sql
        assert "DESTRUCTIVE" in sql
        assert '-- DROP TABLE "old_table";' in sql

    def test_add_column(self):
        diff = SchemaDiff(
            added_columns=[
                ("users", Column("email", "TEXT", nullable=False, default="''"))
            ]
        )
        sql = generate_migration_sql(diff, Dialect.POSTGRES)
        assert "ALTER TABLE" in sql
        assert "ADD COLUMN" in sql
        assert '"email"' in sql

    def test_drop_column_is_commented(self):
        diff = SchemaDiff(dropped_columns=[("users", "old_col")])
        sql = generate_migration_sql(diff, Dialect.POSTGRES)
        assert "WARNING" in sql
        assert '-- ALTER TABLE "users" DROP COLUMN "old_col";' in sql

    def test_alter_column_postgres_type(self):
        diff = SchemaDiff(
            altered_columns=[
                (
                    "users",
                    Column("age", "INTEGER"),
                    Column("age", "BIGINT"),
                )
            ]
        )
        sql = generate_migration_sql(diff, Dialect.POSTGRES)
        assert "ALTER COLUMN" in sql
        assert "TYPE BIGINT" in sql

    def test_alter_column_postgres_nullability(self):
        diff = SchemaDiff(
            altered_columns=[
                (
                    "users",
                    Column("name", "TEXT", nullable=True),
                    Column("name", "TEXT", nullable=False),
                )
            ]
        )
        sql = generate_migration_sql(diff, Dialect.POSTGRES)
        assert "SET NOT NULL" in sql

    def test_alter_column_postgres_drop_not_null(self):
        diff = SchemaDiff(
            altered_columns=[
                (
                    "users",
                    Column("name", "TEXT", nullable=False),
                    Column("name", "TEXT", nullable=True),
                )
            ]
        )
        sql = generate_migration_sql(diff, Dialect.POSTGRES)
        assert "DROP NOT NULL" in sql

    def test_alter_column_mysql_uses_modify(self):
        diff = SchemaDiff(
            altered_columns=[
                (
                    "users",
                    Column("age", "INTEGER"),
                    Column("age", "BIGINT"),
                )
            ]
        )
        sql = generate_migration_sql(diff, Dialect.MYSQL)
        assert "MODIFY COLUMN" in sql

    def test_alter_column_sqlite_warns(self):
        diff = SchemaDiff(
            altered_columns=[
                (
                    "users",
                    Column("age", "INTEGER"),
                    Column("age", "BIGINT"),
                )
            ]
        )
        sql = generate_migration_sql(diff, Dialect.SQLITE)
        assert "does not support ALTER COLUMN" in sql
        assert "table rebuild pattern" in sql

    def test_create_index(self):
        diff = SchemaDiff(
            new_indexes=[
                Index(
                    name="idx_email",
                    table="users",
                    columns=["email"],
                    unique=True,
                )
            ]
        )
        sql = generate_migration_sql(diff, Dialect.POSTGRES)
        assert "CREATE UNIQUE INDEX" in sql
        assert '"idx_email"' in sql

    def test_drop_index_postgres(self):
        diff = SchemaDiff(dropped_indexes=[("users", "idx_old")])
        sql = generate_migration_sql(diff, Dialect.POSTGRES)
        assert 'DROP INDEX "idx_old";' in sql

    def test_drop_index_mysql_includes_table(self):
        diff = SchemaDiff(dropped_indexes=[("users", "idx_old")])
        sql = generate_migration_sql(diff, Dialect.MYSQL)
        assert "DROP INDEX `idx_old` ON `users`" in sql

    def test_sqlite_rebuild_pattern_included(self):
        diff = SchemaDiff(
            altered_columns=[
                (
                    "users",
                    Column("age", "INTEGER"),
                    Column("age", "BIGINT"),
                )
            ]
        )
        sql = generate_migration_sql(diff, Dialect.SQLITE)
        assert "SQLite Table Rebuild Pattern" in sql
        assert "RENAME TO" in sql

    def test_add_foreign_key_postgres(self):
        diff = SchemaDiff(
            new_foreign_keys=[
                ForeignKey(
                    name="fk_order",
                    table="items",
                    columns=["order_id"],
                    ref_table="orders",
                    ref_columns=["id"],
                )
            ]
        )
        sql = generate_migration_sql(diff, Dialect.POSTGRES)
        assert "ADD CONSTRAINT" in sql
        assert "FOREIGN KEY" in sql
        assert "REFERENCES" in sql

    def test_add_foreign_key_sqlite_warns(self):
        diff = SchemaDiff(
            new_foreign_keys=[
                ForeignKey(
                    name="fk_order",
                    table="items",
                    columns=["order_id"],
                    ref_table="orders",
                    ref_columns=["id"],
                )
            ]
        )
        sql = generate_migration_sql(diff, Dialect.SQLITE)
        assert "does not support ADD CONSTRAINT" in sql

    def test_dialect_header(self):
        diff = SchemaDiff()
        for dialect in Dialect:
            sql = generate_migration_sql(diff, dialect)
            assert f"Dialect: {dialect.value}" in sql


# ── End-to-end: parse + diff + generate ────────────────────────────────


class TestEndToEnd:
    def test_add_column_via_sql(self):
        source_sql = """
        CREATE TABLE users (
            id INTEGER NOT NULL,
            name TEXT
        );
        """
        target_sql = """
        CREATE TABLE users (
            id INTEGER NOT NULL,
            name TEXT,
            email TEXT NOT NULL
        );
        """
        src = parse_create_statements(source_sql)
        tgt = parse_create_statements(target_sql)
        diff = diff_schemas(src, tgt)
        migration = generate_migration_sql(diff, Dialect.POSTGRES)
        assert "ADD COLUMN" in migration
        assert '"email"' in migration

    def test_new_table_via_sql(self):
        source_sql = """
        CREATE TABLE users (id INTEGER NOT NULL);
        """
        target_sql = """
        CREATE TABLE users (id INTEGER NOT NULL);
        CREATE TABLE orders (id INTEGER NOT NULL, user_id INTEGER);
        """
        src = parse_create_statements(source_sql)
        tgt = parse_create_statements(target_sql)
        diff = diff_schemas(src, tgt)
        migration = generate_migration_sql(diff, Dialect.POSTGRES)
        assert 'CREATE TABLE "orders"' in migration

    def test_full_migration_mysql(self):
        source_sql = """
        CREATE TABLE products (
            id INTEGER NOT NULL,
            name VARCHAR(100),
            price DECIMAL(10,2)
        );
        """
        target_sql = """
        CREATE TABLE products (
            id INTEGER NOT NULL,
            name VARCHAR(255),
            price DECIMAL(10,2),
            sku TEXT NOT NULL
        );
        CREATE INDEX idx_products_sku ON products (sku);
        """
        src = parse_create_statements(source_sql)
        tgt = parse_create_statements(target_sql)
        diff = diff_schemas(src, tgt)
        migration = generate_migration_sql(diff, Dialect.MYSQL)
        assert "ADD COLUMN" in migration
        assert "`sku`" in migration
        assert "MODIFY COLUMN" in migration
        assert "CREATE INDEX" in migration
