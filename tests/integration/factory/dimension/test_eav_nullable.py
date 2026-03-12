"""Integration tests for EAV nullable attribute enforcement.

These tests render the EAV trigger templates into real PL/pgSQL,
install them against a live PostgreSQL instance, and verify that:

- Non-nullable attributes raise an exception on INSERT/UPDATE when
  the value is omitted (NULL).
- Nullable attributes silently allow NULL (no row is written to the
  attribute table).
- Updates that do not change a value write no new attribute row.
"""

import pytest
from sqlalchemy import String, text

from cave.factory.dimension.eav import _EAVMapping
from cave.utils.template import load_template


def _render_insert(mappings: list[_EAVMapping], schema: str) -> str:
    tpl = load_template("eav_insert.mako")
    return tpl.render(
        entity_table=f"{schema}.things_entity",
        attr_table=f"{schema}.things_attribute",
        mappings=[
            (m.attribute_name, m.value_column, m.nullable) for m in mappings
        ],
    )


def _render_update(mappings: list[_EAVMapping], schema: str) -> str:
    tpl = load_template("eav_update.mako")
    return tpl.render(
        attr_table=f"{schema}.things_attribute",
        mappings=[
            (m.attribute_name, m.value_column, m.nullable) for m in mappings
        ],
    )


def _setup_eav(conn, schema: str, mappings: list[_EAVMapping]) -> None:
    """Create entity/attribute tables, pivot view, and INSTEAD OF triggers."""
    value_cols = dict.fromkeys(m.value_column for m in mappings)
    value_col_defs = "\n".join(f"    {vc} TEXT," for vc in value_cols)

    conn.execute(
        text(f"""
        CREATE TABLE {schema}.things_entity (
            id SERIAL PRIMARY KEY,
            created_at TIMESTAMPTZ DEFAULT now()
        )
    """)
    )
    conn.execute(
        text(f"""
        CREATE TABLE {schema}.things_attribute (
            id SERIAL PRIMARY KEY,
            entity_id INTEGER NOT NULL
                REFERENCES {schema}.things_entity(id) ON DELETE CASCADE,
            attribute_name TEXT NOT NULL,
            {value_col_defs}
            created_at TIMESTAMPTZ DEFAULT now()
        )
    """)
    )

    pivot_cols = ",\n".join(
        f"    MAX(a.{m.value_column}) FILTER "
        f"(WHERE a.attribute_name = '{m.attribute_name}') "
        f"AS {m.attribute_name}"
        for m in mappings
    )
    conn.execute(
        text(f"""
        CREATE VIEW {schema}.things AS
        SELECT e.id, e.created_at,
        {pivot_cols}
        FROM {schema}.things_entity e
        LEFT JOIN (
            SELECT DISTINCT ON (entity_id, attribute_name) *
            FROM {schema}.things_attribute
            ORDER BY entity_id, attribute_name, created_at DESC, id DESC
        ) a ON a.entity_id = e.id
        GROUP BY e.id, e.created_at
    """)
    )

    insert_body = _render_insert(mappings, schema)
    conn.execute(
        text(f"""
        CREATE FUNCTION {schema}.things_insert() RETURNS trigger
        LANGUAGE plpgsql AS $$ {insert_body} $$
    """)
    )
    conn.execute(
        text(f"""
        CREATE TRIGGER things_insert
        INSTEAD OF INSERT ON {schema}.things
        FOR EACH ROW EXECUTE FUNCTION {schema}.things_insert()
    """)
    )

    update_body = _render_update(mappings, schema)
    conn.execute(
        text(f"""
        CREATE FUNCTION {schema}.things_update() RETURNS trigger
        LANGUAGE plpgsql AS $$ {update_body} $$
    """)
    )
    conn.execute(
        text(f"""
        CREATE TRIGGER things_update
        INSTEAD OF UPDATE ON {schema}.things
        FOR EACH ROW EXECUTE FUNCTION {schema}.things_update()
    """)
    )


@pytest.fixture
def eav_nullable(db_conn, db_schema):
    """EAV setup with one required (sku) and one optional (color) attribute."""
    mappings = [
        _EAVMapping("sku", "text_value", String(), nullable=False),
        _EAVMapping("color", "text_value", String(), nullable=True),
    ]
    _setup_eav(db_conn, db_schema, mappings)
    return db_conn, db_schema


class TestInsertEnforcement:
    def test_insert_with_required_attribute_succeeds(self, eav_nullable):
        conn, schema = eav_nullable
        conn.execute(
            text(f"INSERT INTO {schema}.things (sku) VALUES ('ABC-1')")
        )
        row = conn.execute(text(f"SELECT sku FROM {schema}.things")).fetchone()
        assert row is not None
        assert row[0] == "ABC-1"

    def test_insert_missing_required_attribute_raises(self, eav_nullable):
        conn, schema = eav_nullable
        with pytest.raises(Exception, match="sku"):
            conn.execute(
                text(f"INSERT INTO {schema}.things (color) VALUES ('red')")
            )

    def test_insert_with_null_optional_attribute_succeeds(self, eav_nullable):
        conn, schema = eav_nullable
        conn.execute(
            text(
                f"INSERT INTO {schema}.things (sku, color)"
                f" VALUES ('ABC-2', NULL)"
            )
        )
        row = conn.execute(
            text(f"SELECT sku, color FROM {schema}.things")
        ).fetchone()
        assert row is not None
        assert row[0] == "ABC-2"
        assert row[1] is None

    def test_insert_omitting_optional_attribute_succeeds(self, eav_nullable):
        conn, schema = eav_nullable
        conn.execute(
            text(f"INSERT INTO {schema}.things (sku) VALUES ('ABC-3')")
        )
        row = conn.execute(
            text(f"SELECT color FROM {schema}.things")
        ).fetchone()
        assert row is not None
        assert row[0] is None


class TestUpdateEnforcement:
    @pytest.fixture(autouse=True)
    def _seed(self, eav_nullable):
        conn, schema = eav_nullable
        conn.execute(
            text(
                f"INSERT INTO {schema}.things (sku, color)"
                f" VALUES ('ABC-10', 'red')"
            )
        )
        self.conn = conn
        self.schema = schema

    def test_update_required_attribute_to_null_raises(self):
        with pytest.raises(Exception, match="sku"):
            self.conn.execute(
                text(
                    f"UPDATE {self.schema}.things SET sku = NULL"
                    f" WHERE sku = 'ABC-10'"
                )
            )

    def test_update_with_same_value_writes_no_new_row(self):
        before = self.conn.execute(
            text(
                f"SELECT COUNT(*) FROM {self.schema}.things_attribute"
                f" WHERE attribute_name = 'color'"
            )
        ).scalar()
        self.conn.execute(
            text(
                f"UPDATE {self.schema}.things SET color = 'red'"
                f" WHERE sku = 'ABC-10'"
            )
        )
        after = self.conn.execute(
            text(
                f"SELECT COUNT(*) FROM {self.schema}.things_attribute"
                f" WHERE attribute_name = 'color'"
            )
        ).scalar()
        assert before == after

    def test_update_with_changed_value_writes_new_row(self):
        before = self.conn.execute(
            text(
                f"SELECT COUNT(*) FROM {self.schema}.things_attribute"
                f" WHERE attribute_name = 'color'"
            )
        ).scalar()
        self.conn.execute(
            text(
                f"UPDATE {self.schema}.things SET color = 'blue'"
                f" WHERE sku = 'ABC-10'"
            )
        )
        after = self.conn.execute(
            text(
                f"SELECT COUNT(*) FROM {self.schema}.things_attribute"
                f" WHERE attribute_name = 'color'"
            )
        ).scalar()
        assert after == before + 1
