"""Integration tests for EAV check constraints via TriggerCheckPlugin.

Creates real EAV tables, a pivot view, check-enforcement triggers
(firing before the main EAV triggers), and the main EAV triggers,
then verifies that:

- Valid inserts succeed.
- Inserts violating a single-column check are rejected.
- Inserts violating a multi-column check are rejected.
- Updates that violate a check are rejected.
- Updates that satisfy the check succeed.
"""

from pathlib import Path

import pytest
from sqlalchemy import Integer, text

from cave.check import CaveCheck
from cave.plugins.eav import _EAVMapping
from cave.utils.template import load_template

_EAV_TEMPLATES = (
    Path(__file__).resolve().parents[4]
    / "src"
    / "cave"
    / "plugins"
    / "templates"
    / "eav"
)

_CHECK_TEMPLATES = (
    Path(__file__).resolve().parents[4]
    / "src"
    / "cave"
    / "plugins"
    / "templates"
    / "check"
)


def _render_check_validate(
    checks: list[CaveCheck],
) -> str:
    """Render the check-enforcement trigger body."""
    tpl = load_template(_CHECK_TEMPLATES / "validate.mako")
    resolved = [
        (
            check.resolve(lambda c: f"NEW.{c}"),
            check.name,
        )
        for check in checks
    ]
    return tpl.render(checks=resolved)


def _render_eav_insert(mappings: list[_EAVMapping], schema: str) -> str:
    tpl = load_template(_EAV_TEMPLATES / "insert.mako")
    return tpl.render(
        entity_table=f"{schema}.products_entity",
        attr_table=f"{schema}.products_attribute",
        mappings=[
            (m.attribute_name, m.value_column, m.nullable) for m in mappings
        ],
    )


def _render_eav_update(mappings: list[_EAVMapping], schema: str) -> str:
    tpl = load_template(_EAV_TEMPLATES / "update.mako")
    return tpl.render(
        attr_table=f"{schema}.products_attribute",
        mappings=[
            (m.attribute_name, m.value_column, m.nullable) for m in mappings
        ],
    )


def _setup_eav_with_checks(
    conn,
    schema: str,
    mappings: list[_EAVMapping],
    checks: list[CaveCheck],
) -> None:
    """Create EAV tables, pivot view, check triggers, and EAV triggers."""
    value_cols = dict.fromkeys(m.value_column for m in mappings)
    value_col_defs = "\n".join(f"    {vc} INTEGER," for vc in value_cols)

    conn.execute(
        text(f"""
        CREATE TABLE {schema}.products_entity (
            id SERIAL PRIMARY KEY,
            created_at TIMESTAMPTZ DEFAULT now()
        )
    """)
    )
    conn.execute(
        text(f"""
        CREATE TABLE {schema}.products_attribute (
            id SERIAL PRIMARY KEY,
            entity_id INTEGER NOT NULL
                REFERENCES {schema}.products_entity(id)
                ON DELETE CASCADE,
            attribute_name TEXT NOT NULL,
            {value_col_defs}
            created_at TIMESTAMPTZ DEFAULT now()
        )
    """)
    )

    # Pivot view
    pivot_cols = ",\n".join(
        f"    MAX(a.{m.value_column}) FILTER "
        f"(WHERE a.attribute_name = '{m.attribute_name}') "
        f"AS {m.attribute_name}"
        for m in mappings
    )
    conn.execute(
        text(f"""
        CREATE VIEW {schema}.products AS
        SELECT e.id, e.created_at,
        {pivot_cols}
        FROM {schema}.products_entity e
        LEFT JOIN (
            SELECT DISTINCT ON (entity_id, attribute_name) *
            FROM {schema}.products_attribute
            ORDER BY entity_id, attribute_name,
                     created_at DESC, id DESC
        ) a ON a.entity_id = e.id
        GROUP BY e.id, e.created_at
    """)
    )

    # Check-enforcement triggers (fire FIRST due to 00_ prefix)
    if checks:
        check_body = _render_check_validate(checks)
        for op in ("insert", "update"):
            fn_name = f"00_check_{schema}_products_{op}"
            conn.execute(
                text(f"""
                CREATE FUNCTION {schema}."{fn_name}"()
                RETURNS trigger
                LANGUAGE plpgsql AS $$ {check_body} $$
            """)
            )
            conn.execute(
                text(f"""
                CREATE TRIGGER "{fn_name}"
                INSTEAD OF {op.upper()}
                ON {schema}.products
                FOR EACH ROW
                EXECUTE FUNCTION {schema}."{fn_name}"()
            """)
            )

    # Main EAV triggers (fire AFTER check triggers alphabetically)
    insert_body = _render_eav_insert(mappings, schema)
    conn.execute(
        text(f"""
        CREATE FUNCTION {schema}.products_insert()
        RETURNS trigger
        LANGUAGE plpgsql AS $$ {insert_body} $$
    """)
    )
    conn.execute(
        text(f"""
        CREATE TRIGGER products_insert
        INSTEAD OF INSERT ON {schema}.products
        FOR EACH ROW
        EXECUTE FUNCTION {schema}.products_insert()
    """)
    )

    update_body = _render_eav_update(mappings, schema)
    conn.execute(
        text(f"""
        CREATE FUNCTION {schema}.products_update()
        RETURNS trigger
        LANGUAGE plpgsql AS $$ {update_body} $$
    """)
    )
    conn.execute(
        text(f"""
        CREATE TRIGGER products_update
        INSTEAD OF UPDATE ON {schema}.products
        FOR EACH ROW
        EXECUTE FUNCTION {schema}.products_update()
    """)
    )


@pytest.fixture
def eav_with_checks(db_conn, db_schema):
    """EAV with price (>0) and qty (>=0) columns plus a cross-column check."""
    mappings = [
        _EAVMapping("price", "integer_value", Integer(), nullable=False),
        _EAVMapping("qty", "integer_value", Integer(), nullable=False),
    ]
    checks = [
        CaveCheck("{price} > 0", name="positive_price"),
        CaveCheck("{qty} >= 0", name="nonneg_qty"),
        CaveCheck(
            "{price} * {qty} <= 1000000",
            name="max_total",
        ),
    ]
    _setup_eav_with_checks(db_conn, db_schema, mappings, checks)
    return db_conn, db_schema


class TestEAVCheckInsert:
    def test_valid_insert_succeeds(self, eav_with_checks):
        conn, schema = eav_with_checks
        conn.execute(
            text(f"INSERT INTO {schema}.products (price, qty) VALUES (10, 5)")
        )
        row = conn.execute(
            text(f"SELECT price, qty FROM {schema}.products")
        ).fetchone()
        assert row is not None
        assert row[0] == 10
        assert row[1] == 5

    def test_negative_price_rejected(self, eav_with_checks):
        conn, schema = eav_with_checks
        with pytest.raises(Exception, match="positive_price"):
            conn.execute(
                text(
                    f"INSERT INTO {schema}.products (price, qty) VALUES (-1, 5)"
                )
            )

    def test_zero_price_rejected(self, eav_with_checks):
        conn, schema = eav_with_checks
        with pytest.raises(Exception, match="positive_price"):
            conn.execute(
                text(
                    f"INSERT INTO {schema}.products (price, qty) VALUES (0, 5)"
                )
            )

    def test_negative_qty_rejected(self, eav_with_checks):
        conn, schema = eav_with_checks
        with pytest.raises(Exception, match="nonneg_qty"):
            conn.execute(
                text(
                    f"INSERT INTO {schema}.products"
                    f" (price, qty) VALUES (10, -1)"
                )
            )

    def test_cross_column_check_rejected(self, eav_with_checks):
        conn, schema = eav_with_checks
        with pytest.raises(Exception, match="max_total"):
            conn.execute(
                text(
                    f"INSERT INTO {schema}.products"
                    f" (price, qty) VALUES (1001, 1000)"
                )
            )

    def test_cross_column_check_at_boundary_succeeds(self, eav_with_checks):
        conn, schema = eav_with_checks
        conn.execute(
            text(
                f"INSERT INTO {schema}.products"
                f" (price, qty) VALUES (1000, 1000)"
            )
        )
        row = conn.execute(
            text(f"SELECT price, qty FROM {schema}.products")
        ).fetchone()
        assert row[0] == 1000
        assert row[1] == 1000


class TestEAVCheckUpdate:
    @pytest.fixture(autouse=True)
    def _seed(self, eav_with_checks):
        conn, schema = eav_with_checks
        conn.execute(
            text(f"INSERT INTO {schema}.products (price, qty) VALUES (10, 5)")
        )
        self.conn = conn
        self.schema = schema

    def test_update_to_invalid_price_rejected(self):
        with pytest.raises(Exception, match="positive_price"):
            self.conn.execute(
                text(f"UPDATE {self.schema}.products SET price = -1")
            )

    def test_update_to_valid_price_succeeds(self):
        self.conn.execute(text(f"UPDATE {self.schema}.products SET price = 20"))
        row = self.conn.execute(
            text(f"SELECT price FROM {self.schema}.products")
        ).fetchone()
        assert row[0] == 20

    def test_update_violating_cross_column_check_rejected(
        self,
    ):
        with pytest.raises(Exception, match="max_total"):
            self.conn.execute(
                text(
                    f"UPDATE {self.schema}.products"
                    f" SET price = 1001, qty = 1000"
                )
            )
