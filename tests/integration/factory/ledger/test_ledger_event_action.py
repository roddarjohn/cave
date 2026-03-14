"""Integration tests for ledger EventAction functions.

Creates real database objects and exercises the record function
directly in SQL, independent of the full factory pipeline.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError

# ---------------------------------------------------------------------------
# Setup helpers
# ---------------------------------------------------------------------------


def _setup_event_ledger(  # noqa: PLR0913
    conn,
    schema: str,
    tablename: str,
    dim_keys: list[str],
    dim_types: list[str],
    write_only_cols: list[str] | None = None,
    write_only_types: list[str] | None = None,
) -> None:
    """Create a ledger table, API view, INSERT trigger, and record fn."""
    write_only_cols = write_only_cols or []
    write_only_types = write_only_types or []

    base = f"{schema}.{tablename}"
    view = f"{schema}.api_{tablename}"

    dim_col_defs = "".join(
        f", {k} {t} NOT NULL" for k, t in zip(dim_keys, dim_types, strict=False)
    )
    wo_col_defs = "".join(
        f", {k} {t}"
        for k, t in zip(write_only_cols, write_only_types, strict=False)
    )

    conn.execute(
        text(f"""
        CREATE TABLE {base} (
            id SERIAL PRIMARY KEY,
            entry_id UUID NOT NULL DEFAULT gen_random_uuid(),
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            value INTEGER NOT NULL
            {dim_col_defs}
            {wo_col_defs}
        )
    """)
    )

    all_cols = ", ".join(
        ["id", "entry_id", "created_at", "value", *dim_keys, *write_only_cols]
    )
    conn.execute(text(f"CREATE VIEW {view} AS SELECT {all_cols} FROM {base}"))

    # INSERT INSTEAD OF trigger (enforces immutability).
    ins_cols = ", ".join(["entry_id", "value", *dim_keys, *write_only_cols])
    ins_new = ", ".join(
        [
            "COALESCE(NEW.entry_id, gen_random_uuid())",
            "NEW.value",
            *[f"NEW.{k}" for k in dim_keys],
            *[f"NEW.{k}" for k in write_only_cols],
        ]
    )
    conn.execute(
        text(f"""
        CREATE FUNCTION {schema}.{tablename}_ins()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            INSERT INTO {base} ({ins_cols}) VALUES ({ins_new})
            RETURNING * INTO NEW;
            RETURN NEW;
        END; $$
    """)
    )
    conn.execute(
        text(f"""
        CREATE TRIGGER {tablename}_ins
        INSTEAD OF INSERT ON {view}
        FOR EACH ROW EXECUTE FUNCTION {schema}.{tablename}_ins()
    """)
    )

    # UPDATE / DELETE reject triggers.
    for op in ("update", "delete"):
        conn.execute(
            text(f"""
            CREATE FUNCTION {schema}.{tablename}_{op}()
            RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                RAISE EXCEPTION 'cannot {op} immutable ledger entries';
            END; $$
        """)
        )
        conn.execute(
            text(f"""
            CREATE TRIGGER {tablename}_{op}
            INSTEAD OF {op.upper()} ON {view}
            FOR EACH ROW EXECUTE FUNCTION {schema}.{tablename}_{op}()
        """)
        )

    # EventAction record function.
    all_fn_cols = ", ".join([*dim_keys, *write_only_cols, "value"])
    dim_params = ", ".join(
        f"p_{k} {t}" for k, t in zip(dim_keys, dim_types, strict=False)
    )
    wo_params = (
        ", "
        + ", ".join(
            f"p_{k} {t} DEFAULT NULL"
            for k, t in zip(write_only_cols, write_only_types, strict=False)
        )
        if write_only_cols
        else ""
    )
    all_fn_params_def = f"p_value INTEGER, {dim_params}{wo_params}"
    all_fn_params_use = ", ".join(
        [f"p_{k}" for k in [*dim_keys, *write_only_cols, "value"]]
    )
    conn.execute(
        text(f"""
        CREATE FUNCTION {schema}.{tablename}_record(
            {all_fn_params_def}
        )
        RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS $$
        BEGIN
            INSERT INTO {view} ({all_fn_cols})
            VALUES ({all_fn_params_use});
        END; $$
    """)
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def tx_ledger(db_conn, db_schema):
    """Transaction ledger: dim_keys=[account], no write-only."""
    _setup_event_ledger(
        db_conn,
        db_schema,
        "txn",
        dim_keys=["account"],
        dim_types=["TEXT"],
    )
    return db_conn, db_schema


@pytest.fixture
def tx_ledger_with_note(db_conn, db_schema):
    """Transaction ledger with write-only 'note' column."""
    _setup_event_ledger(
        db_conn,
        db_schema,
        "txn",
        dim_keys=["account"],
        dim_types=["TEXT"],
        write_only_cols=["note"],
        write_only_types=["TEXT"],
    )
    return db_conn, db_schema


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestEventActionRecord:
    def test_inserts_correct_delta_row(self, tx_ledger):
        conn, schema = tx_ledger
        conn.execute(
            text(f"SELECT {schema}.txn_record(:p_value, :p_account)"),
            {"p_value": 42, "p_account": "cash"},
        )
        row = conn.execute(
            text(f"SELECT value, account FROM {schema}.txn")
        ).fetchone()
        assert row is not None
        assert row[0] == 42
        assert row[1] == "cash"

    def test_multiple_calls_accumulate(self, tx_ledger):
        conn, schema = tx_ledger
        conn.execute(
            text(f"SELECT {schema}.txn_record(:v, :a)"),
            {"v": 10, "a": "cash"},
        )
        conn.execute(
            text(f"SELECT {schema}.txn_record(:v, :a)"),
            {"v": 20, "a": "cash"},
        )
        total = conn.execute(
            text(f"SELECT SUM(value) FROM {schema}.txn WHERE account='cash'")
        ).scalar()
        assert total == 30

    def test_immutability_prevents_update(self, tx_ledger):
        conn, schema = tx_ledger
        conn.execute(
            text(f"SELECT {schema}.txn_record(:v, :a)"),
            {"v": 10, "a": "cash"},
        )
        with pytest.raises(ProgrammingError, match="cannot update"):
            conn.execute(text(f"UPDATE {schema}.api_txn SET value = 999"))

    def test_immutability_prevents_delete(self, tx_ledger):
        conn, schema = tx_ledger
        conn.execute(
            text(f"SELECT {schema}.txn_record(:v, :a)"),
            {"v": 10, "a": "cash"},
        )
        with pytest.raises(ProgrammingError, match="cannot delete"):
            conn.execute(text(f"DELETE FROM {schema}.api_txn"))


class TestEventActionWriteOnly:
    def test_write_only_written_when_provided(self, tx_ledger_with_note):
        conn, schema = tx_ledger_with_note
        conn.execute(
            text(f"SELECT {schema}.txn_record(:v, :a, :n)"),
            {"v": 50, "a": "revenue", "n": "sale"},
        )
        row = conn.execute(
            text(f"SELECT note FROM {schema}.txn WHERE account='revenue'")
        ).fetchone()
        assert row is not None
        assert row[0] == "sale"

    def test_write_only_null_when_omitted(self, tx_ledger_with_note):
        conn, schema = tx_ledger_with_note
        # Call without the optional 'note' param.
        conn.execute(
            text(f"SELECT {schema}.txn_record(:v, :a)"),
            {"v": 10, "a": "expense"},
        )
        row = conn.execute(
            text(f"SELECT note FROM {schema}.txn WHERE account='expense'")
        ).fetchone()
        assert row is not None
        assert row[0] is None
