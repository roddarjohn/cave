"""Integration tests for RawTableProtectionPlugin.

Verifies that BEFORE triggers block direct DML on raw backing tables
while still allowing mutations that arrive through INSTEAD OF triggers
on the API view (pg_trigger_depth() > 0).
"""

import pytest
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError

from pgcraft.plugins.protect import _PROTECTION_FUNCTION_BODY


def _setup_protected_table(conn, schema: str, tablename: str) -> None:
    """Create a backing table with protection triggers and a simple API view.

    The API view has INSTEAD OF INSERT/UPDATE/DELETE triggers that
    route to the backing table, mirroring what SimpleTriggerPlugin
    produces in production.
    """
    base_table = f"{schema}.{tablename}"
    api_view = f"{schema}.api_{tablename}"

    conn.execute(
        text(f"""
        CREATE TABLE {base_table} (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL
        )
    """)
    )

    # Protection triggers on the backing table.
    fn_name = f"_protect_{schema}_{tablename}"
    conn.execute(
        text(f"""
        CREATE FUNCTION {schema}.{fn_name}() RETURNS trigger
        LANGUAGE plpgsql AS $$ {_PROTECTION_FUNCTION_BODY} $$
    """)
    )
    for op in ("INSERT", "UPDATE", "DELETE"):
        conn.execute(
            text(f"""
            CREATE TRIGGER _protect_{schema}_{tablename}_{op.lower()}
            BEFORE {op} ON {base_table}
            FOR EACH ROW EXECUTE FUNCTION {schema}.{fn_name}()
        """)
        )

    # API view with INSTEAD OF triggers that route to the backing table.
    conn.execute(
        text(f"""
        CREATE VIEW {api_view} AS SELECT id, name FROM {base_table}
    """)
    )

    conn.execute(
        text(f"""
        CREATE FUNCTION {schema}.api_{tablename}_insert() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            INSERT INTO {base_table} (name) VALUES (NEW.name)
            RETURNING * INTO NEW;
            RETURN NEW;
        END; $$
    """)
    )
    conn.execute(
        text(f"""
        CREATE TRIGGER api_{tablename}_insert
        INSTEAD OF INSERT ON {api_view}
        FOR EACH ROW EXECUTE FUNCTION {schema}.api_{tablename}_insert()
    """)
    )

    conn.execute(
        text(f"""
        CREATE FUNCTION {schema}.api_{tablename}_update() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            UPDATE {base_table} SET name = NEW.name WHERE id = OLD.id;
            RETURN NEW;
        END; $$
    """)
    )
    conn.execute(
        text(f"""
        CREATE TRIGGER api_{tablename}_update
        INSTEAD OF UPDATE ON {api_view}
        FOR EACH ROW EXECUTE FUNCTION {schema}.api_{tablename}_update()
    """)
    )

    conn.execute(
        text(f"""
        CREATE FUNCTION {schema}.api_{tablename}_delete() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            DELETE FROM {base_table} WHERE id = OLD.id;
            RETURN OLD;
        END; $$
    """)
    )
    conn.execute(
        text(f"""
        CREATE TRIGGER api_{tablename}_delete
        INSTEAD OF DELETE ON {api_view}
        FOR EACH ROW EXECUTE FUNCTION {schema}.api_{tablename}_delete()
    """)
    )


@pytest.fixture
def protected_dim(db_conn, db_schema):
    """Set up a protected 'items' dimension in the test schema."""
    _setup_protected_table(db_conn, db_schema, "items")
    return db_conn, db_schema


class TestDirectDmlBlocked:
    """Direct DML on the raw backing table must raise an exception."""

    def test_direct_insert_raises(self, protected_dim):
        conn, schema = protected_dim
        with pytest.raises(ProgrammingError, match="not allowed"):
            conn.execute(
                text(f"INSERT INTO {schema}.items (name) VALUES ('x')")
            )

    def test_direct_update_raises(self, protected_dim):
        conn, schema = protected_dim
        # Seed a row by bypassing through the API view.
        conn.execute(
            text(f"INSERT INTO {schema}.api_items (name) VALUES ('original')")
        )
        with pytest.raises(ProgrammingError, match="not allowed"):
            conn.execute(
                text(
                    f"UPDATE {schema}.items SET name = 'changed'"
                    f" WHERE name = 'original'"
                )
            )

    def test_direct_delete_raises(self, protected_dim):
        conn, schema = protected_dim
        conn.execute(
            text(f"INSERT INTO {schema}.api_items (name) VALUES ('to_delete')")
        )
        with pytest.raises(ProgrammingError, match="not allowed"):
            conn.execute(
                text(f"DELETE FROM {schema}.items WHERE name = 'to_delete'")
            )


class TestApiViewDmlAllowed:
    """DML through the API view must succeed (pg_trigger_depth() > 0)."""

    def test_insert_via_api_view_succeeds(self, protected_dim):
        conn, schema = protected_dim
        conn.execute(
            text(f"INSERT INTO {schema}.api_items (name) VALUES ('gadget')")
        )
        row = conn.execute(
            text(f"SELECT name FROM {schema}.api_items")
        ).fetchone()
        assert row is not None
        assert row[0] == "gadget"

    def test_update_via_api_view_succeeds(self, protected_dim):
        conn, schema = protected_dim
        conn.execute(
            text(f"INSERT INTO {schema}.api_items (name) VALUES ('old')")
        )
        conn.execute(
            text(
                f"UPDATE {schema}.api_items SET name = 'new' WHERE name = 'old'"
            )
        )
        row = conn.execute(
            text(f"SELECT name FROM {schema}.api_items")
        ).fetchone()
        assert row is not None
        assert row[0] == "new"

    def test_delete_via_api_view_succeeds(self, protected_dim):
        conn, schema = protected_dim
        conn.execute(
            text(f"INSERT INTO {schema}.api_items (name) VALUES ('gone')")
        )
        conn.execute(
            text(f"DELETE FROM {schema}.api_items WHERE name = 'gone'")
        )
        count = conn.execute(
            text(f"SELECT COUNT(*) FROM {schema}.api_items")
        ).scalar()
        assert count == 0

    def test_error_message_names_table(self, protected_dim):
        """Exception message must name the schema and table."""
        conn, schema = protected_dim
        with pytest.raises(ProgrammingError) as exc_info:
            conn.execute(
                text(f"INSERT INTO {schema}.items (name) VALUES ('x')")
            )
        msg = str(exc_info.value)
        assert "items" in msg
