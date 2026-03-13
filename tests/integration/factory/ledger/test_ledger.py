"""Integration tests for LedgerResourceFactory.

Creates real database objects and verifies insert-only operations
through the API view and its INSTEAD OF trigger.
"""

from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError

from pgcraft.utils.template import load_template

_LEDGER_TEMPLATES = (
    Path(__file__).resolve().parents[4]
    / "src"
    / "pgcraft"
    / "plugins"
    / "templates"
    / "ledger"
)


def _render_ledger_insert(base_table: str, cols: str, new_cols: str) -> str:
    tpl = load_template(_LEDGER_TEMPLATES / "insert.mako")
    return tpl.render(
        base_table=base_table,
        cols=cols,
        new_cols=new_cols,
    )


def _setup_ledger(
    conn,
    schema: str,
    tablename: str,
    value_type: str = "INTEGER",
    extra_cols: str = "",
) -> None:
    """Create a ledger table, API view, and INSERT trigger."""
    base_table = f"{schema}.{tablename}"
    api_view = f"{schema}.api_{tablename}"

    extra = f", {extra_cols}" if extra_cols else ""
    conn.execute(
        text(f"""
        CREATE TABLE {base_table} (
            id SERIAL PRIMARY KEY,
            entry_id UUID NOT NULL DEFAULT gen_random_uuid(),
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            value {value_type} NOT NULL
            {extra}
        )
    """)
    )

    cols_select = "id, entry_id, created_at, value"
    if extra_cols:
        col_names = [c.strip().split()[0] for c in extra_cols.split(",")]
        cols_select += ", " + ", ".join(col_names)

    conn.execute(
        text(f"""
        CREATE VIEW {api_view} AS
        SELECT {cols_select} FROM {base_table}
    """)
    )

    # Build trigger column list.
    trigger_cols = ["entry_id", "value"]
    if extra_cols:
        trigger_cols.extend(c.strip().split()[0] for c in extra_cols.split(","))
    cols_str = ", ".join(trigger_cols)
    new_cols_str = ", ".join(f"NEW.{c}" for c in trigger_cols)

    insert_body = _render_ledger_insert(
        base_table=base_table,
        cols=cols_str,
        new_cols=new_cols_str,
    )
    conn.execute(
        text(f"""
        CREATE FUNCTION {schema}.{tablename}_insert()
        RETURNS trigger
        LANGUAGE plpgsql AS $$ {insert_body} $$
    """)
    )
    conn.execute(
        text(f"""
        CREATE TRIGGER {tablename}_insert
        INSTEAD OF INSERT ON {api_view}
        FOR EACH ROW
        EXECUTE FUNCTION {schema}.{tablename}_insert()
    """)
    )


@pytest.fixture
def ledger(db_conn, db_schema):
    """Set up an integer ledger in the test schema."""
    _setup_ledger(
        db_conn,
        db_schema,
        "transactions",
        extra_cols="category TEXT NOT NULL",
    )
    return db_conn, db_schema


@pytest.fixture
def numeric_ledger(db_conn, db_schema):
    """Set up a numeric ledger in the test schema."""
    _setup_ledger(
        db_conn,
        db_schema,
        "payments",
        value_type="NUMERIC",
    )
    return db_conn, db_schema


class TestLedgerInsert:
    def test_insert_via_api_view_succeeds(self, ledger):
        conn, schema = ledger
        conn.execute(
            text(
                f"INSERT INTO {schema}.api_transactions"
                f" (value, category)"
                f" VALUES (100, 'sales')"
            )
        )

    def test_value_and_created_at_populated(self, ledger):
        conn, schema = ledger
        conn.execute(
            text(
                f"INSERT INTO {schema}.api_transactions"
                f" (value, category)"
                f" VALUES (42, 'refund')"
            )
        )
        row = conn.execute(
            text(
                f"SELECT value, created_at, category, entry_id"
                f" FROM {schema}.api_transactions"
            )
        ).fetchone()
        assert row is not None
        assert row[0] == 42
        assert row[1] is not None
        assert row[2] == "refund"
        assert row[3] is not None  # entry_id auto-generated

    def test_explicit_entry_id_correlates_rows(self, ledger):
        conn, schema = ledger
        eid = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
        conn.execute(
            text(
                f"INSERT INTO {schema}.api_transactions"
                f" (entry_id, value, category)"
                f" VALUES ('{eid}', 10, 'a'), ('{eid}', 20, 'b')"
            )
        )
        rows = conn.execute(
            text(
                f"SELECT entry_id FROM {schema}.api_transactions ORDER BY value"
            )
        ).fetchall()
        assert len(rows) == 2
        assert str(rows[0][0]) == eid
        assert str(rows[1][0]) == eid

    def test_multiple_inserts(self, ledger):
        conn, schema = ledger
        conn.execute(
            text(
                f"INSERT INTO {schema}.api_transactions"
                f" (value, category)"
                f" VALUES (10, 'a'), (20, 'b'), (30, 'c')"
            )
        )
        count = conn.execute(
            text(f"SELECT COUNT(*) FROM {schema}.api_transactions")
        ).scalar()
        assert count == 3


class TestLedgerImmutability:
    @pytest.fixture(autouse=True)
    def _seed(self, ledger):
        conn, schema = ledger
        conn.execute(
            text(
                f"INSERT INTO {schema}.api_transactions"
                f" (value, category)"
                f" VALUES (100, 'sales')"
            )
        )
        self.conn = conn
        self.schema = schema

    def test_update_raises(self):
        with pytest.raises(ProgrammingError, match="cannot update"):
            self.conn.execute(
                text(f"UPDATE {self.schema}.api_transactions SET value = 200")
            )

    def test_delete_raises(self):
        with pytest.raises(ProgrammingError, match="cannot delete"):
            self.conn.execute(
                text(f"DELETE FROM {self.schema}.api_transactions")
            )


class TestLedgerNumeric:
    def test_numeric_value_with_decimals(self, numeric_ledger):
        conn, schema = numeric_ledger
        conn.execute(
            text(f"INSERT INTO {schema}.api_payments (value) VALUES (99.95)")
        )
        row = conn.execute(
            text(f"SELECT value FROM {schema}.api_payments")
        ).fetchone()
        assert row is not None
        assert float(row[0]) == pytest.approx(99.95)
