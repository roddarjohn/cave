"""Integration tests for LedgerEvent SQL functions.

Creates real database objects and exercises the generated functions
directly in SQL.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text

# -------------------------------------------------------------------
# Setup helpers
# -------------------------------------------------------------------


def _setup_event_ledger(  # noqa: PLR0913
    conn,
    schema: str,
    tablename: str,
    dim_keys: list[str],
    dim_types: list[str],
    extra_cols: list[str] | None = None,
    extra_types: list[str] | None = None,
) -> None:
    """Create table, API view, INSERT trigger."""
    extra_cols = extra_cols or []
    extra_types = extra_types or []

    base = f"{schema}.{tablename}"
    view = f"{schema}.api_{tablename}"

    dim_col_defs = "".join(
        f", {k} {t} NOT NULL" for k, t in zip(dim_keys, dim_types, strict=False)
    )
    extra_col_defs = "".join(
        f", {k} {t}" for k, t in zip(extra_cols, extra_types, strict=False)
    )

    conn.execute(
        text(f"""
        CREATE TABLE {base} (
            id SERIAL PRIMARY KEY,
            entry_id UUID NOT NULL
                DEFAULT gen_random_uuid(),
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            value INTEGER NOT NULL
            {dim_col_defs}
            {extra_col_defs}
        )
    """)
    )

    all_cols = ", ".join(
        [
            "id",
            "entry_id",
            "created_at",
            "value",
            *dim_keys,
            *extra_cols,
        ]
    )
    conn.execute(text(f"CREATE VIEW {view} AS SELECT {all_cols} FROM {base}"))

    # INSERT INSTEAD OF trigger.
    ins_cols = ", ".join(["entry_id", "value", *dim_keys, *extra_cols])
    ins_new = ", ".join(
        [
            "COALESCE(NEW.entry_id, gen_random_uuid())",
            "NEW.value",
            *[f"NEW.{k}" for k in dim_keys],
            *[f"NEW.{k}" for k in extra_cols],
        ]
    )
    conn.execute(
        text(f"""
        CREATE FUNCTION {schema}.{tablename}_ins()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            INSERT INTO {base} ({ins_cols})
            VALUES ({ins_new})
            RETURNING * INTO NEW;
            RETURN NEW;
        END; $$
    """)
    )
    conn.execute(
        text(f"""
        CREATE TRIGGER {tablename}_ins
        INSTEAD OF INSERT ON {view}
        FOR EACH ROW
        EXECUTE FUNCTION {schema}.{tablename}_ins()
    """)
    )

    # UPDATE / DELETE reject triggers.
    for op in ("update", "delete"):
        conn.execute(
            text(f"""
            CREATE FUNCTION {schema}.{tablename}_{op}()
            RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                RAISE EXCEPTION
                    'cannot {op} immutable ledger entries';
            END; $$
        """)
        )
        conn.execute(
            text(f"""
            CREATE TRIGGER {tablename}_{op}
            INSTEAD OF {op.upper()} ON {view}
            FOR EACH ROW
            EXECUTE FUNCTION {schema}.{tablename}_{op}()
        """)
        )


# -------------------------------------------------------------------
# Fixtures
# -------------------------------------------------------------------


@pytest.fixture
def inv(db_conn, db_schema):
    """Inventory ledger: warehouse + sku dimensions."""
    _setup_event_ledger(
        db_conn,
        db_schema,
        "inventory",
        dim_keys=["warehouse", "sku"],
        dim_types=["TEXT", "TEXT"],
        extra_cols=["source", "reason"],
        extra_types=["TEXT", "TEXT"],
    )
    return db_conn, db_schema


# -------------------------------------------------------------------
# Simple event function helpers
# -------------------------------------------------------------------


def _create_simple_event_fn(  # noqa: PLR0913
    conn, schema, tablename, name, dim_keys, dim_types
) -> None:
    """Create a simple LANGUAGE sql event function."""
    view = f"{schema}.api_{tablename}"
    all_cols = ", ".join([*dim_keys, "value"])

    param_defs = ", ".join(
        f"p_{k} {t}" for k, t in zip(dim_keys, dim_types, strict=False)
    )
    select_cols = ", ".join(f"p_{k} AS {k}" for k in dim_keys)

    conn.execute(
        text(f"""
        CREATE FUNCTION {schema}.{schema}_{tablename}_{name}(
            {param_defs}, p_value INTEGER
        )
        RETURNS SETOF {view}
        LANGUAGE sql SECURITY DEFINER AS $$
            WITH input AS (
                SELECT {select_cols}, p_value AS value
            )
            INSERT INTO {view} ({all_cols})
            SELECT {all_cols} FROM input
            RETURNING *
        $$
    """)
    )


def _create_simple_event_fn_with_extra(  # noqa: PLR0913
    conn,
    schema,
    tablename,
    name,
    dim_keys,
    dim_types,
    extra_cols,
    extra_types,
) -> None:
    """Create a simple event function with extra columns."""
    view = f"{schema}.api_{tablename}"
    all_cols = ", ".join([*dim_keys, "value", *extra_cols])

    param_defs = ", ".join(
        [
            *(f"p_{k} {t}" for k, t in zip(dim_keys, dim_types, strict=False)),
            "p_value INTEGER",
            *(
                f"p_{k} {t}"
                for k, t in zip(extra_cols, extra_types, strict=False)
            ),
        ]
    )
    select_cols = ", ".join(
        [
            *(f"p_{k} AS {k}" for k in dim_keys),
            "p_value AS value",
            *(f"p_{k} AS {k}" for k in extra_cols),
        ]
    )

    conn.execute(
        text(f"""
        CREATE FUNCTION {schema}.{schema}_{tablename}_{name}(
            {param_defs}
        )
        RETURNS SETOF {view}
        LANGUAGE sql SECURITY DEFINER AS $$
            WITH input AS (SELECT {select_cols})
            INSERT INTO {view} ({all_cols})
            SELECT {all_cols} FROM input
            RETURNING *
        $$
    """)
    )


def _create_diff_event_fn(  # noqa: PLR0913
    conn,
    schema,
    tablename,
    name,
    dim_keys,
    dim_types,
    extra_cols=None,
    extra_types=None,
) -> None:
    """Create a diff-mode LANGUAGE sql event function."""
    extra_cols = extra_cols or []
    extra_types = extra_types or []
    base = f"{schema}.{tablename}"
    view = f"{schema}.api_{tablename}"

    all_cols_list = [*dim_keys, "value", *extra_cols]
    all_cols = ", ".join(all_cols_list)

    param_defs = ", ".join(
        [
            *(f"p_{k} {t}" for k, t in zip(dim_keys, dim_types, strict=False)),
            "p_value INTEGER",
            *(
                f"p_{k} {t}"
                for k, t in zip(extra_cols, extra_types, strict=False)
            ),
        ]
    )

    desired_select = ", ".join(
        [
            *(f"p_{k} AS {k}" for k in dim_keys),
            "p_value AS value",
            *(f"p_{k} AS {k}" for k in extra_cols),
        ]
    )

    dk_cols = ", ".join(dim_keys)
    existing_select = (
        f"SELECT {dk_cols}, SUM(value) * -1 AS value "
        f"FROM {base} "
        f"WHERE ({dk_cols}) IN ("
        f"SELECT {dk_cols} FROM desired"
        f") GROUP BY {dk_cols}"
    )

    # Desired cols for union: all columns as-is.
    desired_union_cols = ", ".join(all_cols_list)

    # Existing cols for union: dim_keys + value + NULLs
    # for extra.
    existing_union_parts = [
        *dim_keys,
        "value",
        *[f"NULL AS {c}" for c in extra_cols],
    ]
    existing_union_cols = ", ".join(existing_union_parts)

    # Build GROUP BY + aggregation for deltas CTE.
    group_cols = ", ".join(dim_keys)
    agg_select = ", ".join(
        [
            *(f"combined.{k}" for k in dim_keys),
            "SUM(combined.value) AS value",
            *(f"MAX(combined.{c}) AS {c}" for c in extra_cols),
        ]
    )

    conn.execute(
        text(f"""
        CREATE FUNCTION {schema}.{schema}_{tablename}_{name}(
            {param_defs}
        )
        RETURNS SETOF {view}
        LANGUAGE sql SECURITY DEFINER AS $$
            WITH
              input AS (SELECT {desired_select}),
              desired AS (
                  SELECT {all_cols} FROM input
              ),
              existing AS ({existing_select}),
              deltas AS (
                SELECT {agg_select}
                FROM (
                  SELECT {desired_union_cols} FROM desired
                  UNION ALL
                  SELECT {existing_union_cols} FROM existing
                ) combined
                GROUP BY {group_cols}
                HAVING SUM(combined.value) != 0
              )
            INSERT INTO {view} ({all_cols})
            SELECT {all_cols} FROM deltas
            RETURNING *
        $$
    """)
    )


# -------------------------------------------------------------------
# Tests: simple event
# -------------------------------------------------------------------


class TestSimpleEvent:
    def test_inserts_and_returns_rows(self, inv):
        conn, schema = inv
        _create_simple_event_fn_with_extra(
            conn,
            schema,
            "inventory",
            "adjust",
            dim_keys=["warehouse", "sku"],
            dim_types=["TEXT", "TEXT"],
            extra_cols=["reason"],
            extra_types=["TEXT"],
        )
        rows = conn.execute(
            text(
                f"SELECT * FROM {schema}.{schema}"
                f"_inventory_adjust("
                f"p_warehouse := 'NYC', "
                f"p_sku := 'A', "
                f"p_value := 42, "
                f"p_reason := 'test'"
                f")"
            )
        ).fetchall()
        assert len(rows) == 1
        row = rows[0]
        # Check returned row has correct values.
        assert row.value == 42
        assert row.warehouse == "NYC"
        assert row.sku == "A"

    def test_multiple_calls_accumulate(self, inv):
        conn, schema = inv
        _create_simple_event_fn(
            conn,
            schema,
            "inventory",
            "adjust",
            dim_keys=["warehouse", "sku"],
            dim_types=["TEXT", "TEXT"],
        )
        conn.execute(
            text(
                f"SELECT * FROM {schema}.{schema}"
                f"_inventory_adjust("
                f"p_warehouse := 'NYC', "
                f"p_sku := 'A', "
                f"p_value := 10"
                f")"
            )
        )
        conn.execute(
            text(
                f"SELECT * FROM {schema}.{schema}"
                f"_inventory_adjust("
                f"p_warehouse := 'NYC', "
                f"p_sku := 'A', "
                f"p_value := 20"
                f")"
            )
        )
        total = conn.execute(
            text(
                f"SELECT SUM(value) FROM {schema}.inventory "
                f"WHERE warehouse='NYC' AND sku='A'"
            )
        ).scalar()
        assert total == 30


# -------------------------------------------------------------------
# Tests: diff event (reconciliation)
# -------------------------------------------------------------------


class TestDiffEvent:
    def test_reconciliation_inserts_correcting_deltas(self, inv):
        conn, schema = inv
        # Seed: 50 of A in NYC.
        conn.execute(
            text(
                f"INSERT INTO {schema}.api_inventory "
                f"(warehouse, sku, value) "
                f"VALUES ('NYC', 'A', 50)"
            )
        )

        _create_diff_event_fn(
            conn,
            schema,
            "inventory",
            "reconcile",
            dim_keys=["warehouse", "sku"],
            dim_types=["TEXT", "TEXT"],
            extra_cols=["source"],
            extra_types=["TEXT"],
        )

        # Reconcile to 100.
        rows = conn.execute(
            text(
                f"SELECT * FROM {schema}.{schema}"
                f"_inventory_reconcile("
                f"p_warehouse := 'NYC', "
                f"p_sku := 'A', "
                f"p_value := 100, "
                f"p_source := 'count'"
                f")"
            )
        ).fetchall()
        assert len(rows) == 1
        assert rows[0].value == 50  # delta = 100 - 50

        # Balance should now be 100.
        balance = conn.execute(
            text(
                f"SELECT SUM(value) FROM {schema}.inventory "
                f"WHERE warehouse='NYC' AND sku='A'"
            )
        ).scalar()
        assert balance == 100

    def test_idempotent_reconciliation(self, inv):
        conn, schema = inv
        _create_diff_event_fn(
            conn,
            schema,
            "inventory",
            "reconcile",
            dim_keys=["warehouse", "sku"],
            dim_types=["TEXT", "TEXT"],
        )

        # First call: sets balance to 100.
        conn.execute(
            text(
                f"SELECT * FROM {schema}.{schema}"
                f"_inventory_reconcile("
                f"p_warehouse := 'NYC', "
                f"p_sku := 'A', "
                f"p_value := 100"
                f")"
            )
        )

        # Second call with same desired: no new rows.
        rows = conn.execute(
            text(
                f"SELECT * FROM {schema}.{schema}"
                f"_inventory_reconcile("
                f"p_warehouse := 'NYC', "
                f"p_sku := 'A', "
                f"p_value := 100"
                f")"
            )
        ).fetchall()
        assert len(rows) == 0

    def test_new_combo_inserts_full_value(self, inv):
        conn, schema = inv
        _create_diff_event_fn(
            conn,
            schema,
            "inventory",
            "reconcile",
            dim_keys=["warehouse", "sku"],
            dim_types=["TEXT", "TEXT"],
        )

        rows = conn.execute(
            text(
                f"SELECT * FROM {schema}.{schema}"
                f"_inventory_reconcile("
                f"p_warehouse := 'DFW', "
                f"p_sku := 'NEW', "
                f"p_value := 200"
                f")"
            )
        ).fetchall()
        assert len(rows) == 1
        assert rows[0].value == 200

    def test_zero_desired_with_existing_inserts_negation(self, inv):
        conn, schema = inv
        # Seed: 75 of B in LAX.
        conn.execute(
            text(
                f"INSERT INTO {schema}.api_inventory "
                f"(warehouse, sku, value) "
                f"VALUES ('LAX', 'B', 75)"
            )
        )

        _create_diff_event_fn(
            conn,
            schema,
            "inventory",
            "reconcile",
            dim_keys=["warehouse", "sku"],
            dim_types=["TEXT", "TEXT"],
        )

        # Reconcile to 0.
        rows = conn.execute(
            text(
                f"SELECT * FROM {schema}.{schema}"
                f"_inventory_reconcile("
                f"p_warehouse := 'LAX', "
                f"p_sku := 'B', "
                f"p_value := 0"
                f")"
            )
        ).fetchall()
        # desired=0, existing=-(-75)=... let's check:
        # desired row: value=0, existing row: value=-75
        # union: 0 + (-75) after filter != 0 → only -75
        assert len(rows) == 1
        assert rows[0].value == -75

        balance = conn.execute(
            text(
                f"SELECT SUM(value) FROM {schema}.inventory "
                f"WHERE warehouse='LAX' AND sku='B'"
            )
        ).scalar()
        assert balance == 0
