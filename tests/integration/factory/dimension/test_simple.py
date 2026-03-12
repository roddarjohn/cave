"""Integration tests for SimpleDimensionFactory.

Creates real database objects and verifies CRUD operations through
the API view and its INSTEAD OF triggers.
"""

from pathlib import Path

import pytest
from sqlalchemy import text

from cave.utils.template import load_template

_SIMPLE_TEMPLATES = (
    Path(__file__).resolve().parents[4]
    / "src"
    / "cave"
    / "plugins"
    / "templates"
    / "simple"
)


def _render_simple_insert(
    base_table: str, cols: str, new_cols: str, set_clause: str
) -> str:
    tpl = load_template(_SIMPLE_TEMPLATES / "insert.mako")
    return tpl.render(
        base_table=base_table,
        cols=cols,
        new_cols=new_cols,
        set_clause=set_clause,
    )


def _render_simple_update(
    base_table: str, cols: str, new_cols: str, set_clause: str
) -> str:
    tpl = load_template(_SIMPLE_TEMPLATES / "update.mako")
    return tpl.render(
        base_table=base_table,
        cols=cols,
        new_cols=new_cols,
        set_clause=set_clause,
    )


def _render_simple_delete(
    base_table: str, cols: str, new_cols: str, set_clause: str
) -> str:
    tpl = load_template(_SIMPLE_TEMPLATES / "delete.mako")
    return tpl.render(
        base_table=base_table,
        cols=cols,
        new_cols=new_cols,
        set_clause=set_clause,
    )


def _setup_simple_dimension(
    conn, schema: str, tablename: str, col_defs: str
) -> None:
    """Create a simple dimension table, API view, and INSTEAD OF triggers."""
    base_table = f"{schema}.{tablename}"
    api_view = f"{schema}.api_{tablename}"

    conn.execute(
        text(f"""
        CREATE TABLE {base_table} (
            id SERIAL PRIMARY KEY,
            {col_defs}
        )
    """)
    )

    conn.execute(
        text(f"""
        CREATE VIEW {api_view} AS
        SELECT id, name FROM {base_table}
    """)
    )

    insert_body = _render_simple_insert(
        base_table=base_table,
        cols="name",
        new_cols="NEW.name",
        set_clause="name = NEW.name",
    )
    conn.execute(
        text(f"""
        CREATE FUNCTION {schema}.{tablename}_insert() RETURNS trigger
        LANGUAGE plpgsql AS $$ {insert_body} $$
    """)
    )
    conn.execute(
        text(f"""
        CREATE TRIGGER {tablename}_insert
        INSTEAD OF INSERT ON {api_view}
        FOR EACH ROW EXECUTE FUNCTION {schema}.{tablename}_insert()
    """)
    )

    update_body = _render_simple_update(
        base_table=base_table,
        cols="name",
        new_cols="NEW.name",
        set_clause="name = NEW.name",
    )
    conn.execute(
        text(f"""
        CREATE FUNCTION {schema}.{tablename}_update() RETURNS trigger
        LANGUAGE plpgsql AS $$ {update_body} $$
    """)
    )
    conn.execute(
        text(f"""
        CREATE TRIGGER {tablename}_update
        INSTEAD OF UPDATE ON {api_view}
        FOR EACH ROW EXECUTE FUNCTION {schema}.{tablename}_update()
    """)
    )

    delete_body = _render_simple_delete(
        base_table=base_table,
        cols="name",
        new_cols="NEW.name",
        set_clause="name = NEW.name",
    )
    conn.execute(
        text(f"""
        CREATE FUNCTION {schema}.{tablename}_delete() RETURNS trigger
        LANGUAGE plpgsql AS $$ {delete_body} $$
    """)
    )
    conn.execute(
        text(f"""
        CREATE TRIGGER {tablename}_delete
        INSTEAD OF DELETE ON {api_view}
        FOR EACH ROW EXECUTE FUNCTION {schema}.{tablename}_delete()
    """)
    )


@pytest.fixture
def simple_dim(db_conn, db_schema):
    """Set up a simple 'widgets' dimension in the test schema."""
    _setup_simple_dimension(db_conn, db_schema, "widgets", "name TEXT NOT NULL")
    return db_conn, db_schema


class TestSimpleDimensionInsert:
    def test_insert_via_api_view_succeeds(self, simple_dim):
        conn, schema = simple_dim
        conn.execute(
            text(f"INSERT INTO {schema}.api_widgets (name) VALUES ('Gadget')")
        )

    def test_inserted_row_visible_in_api_view(self, simple_dim):
        conn, schema = simple_dim
        conn.execute(
            text(f"INSERT INTO {schema}.api_widgets (name) VALUES ('Gadget')")
        )
        row = conn.execute(
            text(f"SELECT name FROM {schema}.api_widgets")
        ).fetchone()
        assert row is not None
        assert row[0] == "Gadget"

    def test_inserted_row_visible_in_base_table(self, simple_dim):
        conn, schema = simple_dim
        conn.execute(
            text(f"INSERT INTO {schema}.api_widgets (name) VALUES ('Gadget')")
        )
        row = conn.execute(
            text(f"SELECT name FROM {schema}.widgets")
        ).fetchone()
        assert row is not None
        assert row[0] == "Gadget"

    def test_multiple_inserts(self, simple_dim):
        conn, schema = simple_dim
        conn.execute(
            text(
                f"INSERT INTO {schema}.api_widgets (name)"
                f" VALUES ('A'), ('B'), ('C')"
            )
        )
        count = conn.execute(
            text(f"SELECT COUNT(*) FROM {schema}.api_widgets")
        ).scalar()
        assert count == 3


class TestSimpleDimensionUpdate:
    @pytest.fixture(autouse=True)
    def _seed(self, simple_dim):
        conn, schema = simple_dim
        conn.execute(
            text(f"INSERT INTO {schema}.api_widgets (name) VALUES ('Original')")
        )
        self.conn = conn
        self.schema = schema

    def test_update_via_api_view(self):
        self.conn.execute(
            text(
                f"UPDATE {self.schema}.api_widgets"
                f" SET name = 'Updated' WHERE name = 'Original'"
            )
        )
        row = self.conn.execute(
            text(f"SELECT name FROM {self.schema}.api_widgets")
        ).fetchone()
        assert row is not None
        assert row[0] == "Updated"

    def test_update_reflects_in_base_table(self):
        self.conn.execute(
            text(
                f"UPDATE {self.schema}.api_widgets"
                f" SET name = 'Changed' WHERE name = 'Original'"
            )
        )
        row = self.conn.execute(
            text(f"SELECT name FROM {self.schema}.widgets")
        ).fetchone()
        assert row is not None
        assert row[0] == "Changed"


class TestSimpleDimensionDelete:
    @pytest.fixture(autouse=True)
    def _seed(self, simple_dim):
        conn, schema = simple_dim
        conn.execute(
            text(f"INSERT INTO {schema}.api_widgets (name) VALUES ('ToDelete')")
        )
        self.conn = conn
        self.schema = schema

    def test_delete_via_api_view(self):
        self.conn.execute(
            text(
                f"DELETE FROM {self.schema}.api_widgets WHERE name = 'ToDelete'"
            )
        )
        count = self.conn.execute(
            text(f"SELECT COUNT(*) FROM {self.schema}.api_widgets")
        ).scalar()
        assert count == 0

    def test_delete_removes_from_base_table(self):
        self.conn.execute(
            text(
                f"DELETE FROM {self.schema}.api_widgets WHERE name = 'ToDelete'"
            )
        )
        count = self.conn.execute(
            text(f"SELECT COUNT(*) FROM {self.schema}.widgets")
        ).scalar()
        assert count == 0
