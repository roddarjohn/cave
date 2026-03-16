"""Integration tests for LedgerEvent SQL functions.

Creates real database objects and exercises the generated functions
directly in SQL.  The ledger table setup uses pgcraft's factory;
the event functions are intentionally hand-crafted SQL that tests
the LedgerEvent generation patterns.
"""

from __future__ import annotations

import pytest
from sqlalchemy import Column, MetaData, String, text

from pgcraft.config import PGCraftConfig
from pgcraft.extensions.postgrest import (
    PostgRESTExtension,
    PostgRESTView,
)
from pgcraft.factory.ledger import PGCraftLedger
from tests.integration.conftest import create_all_from_metadata

# -------------------------------------------------------------------
# Fixtures
# -------------------------------------------------------------------


@pytest.fixture
def inv(db_conn, db_schema):
    """Inventory ledger: warehouse + sku dimensions."""
    config = PGCraftConfig(auto_discover=False)
    config.use(PostgRESTExtension())

    metadata = MetaData()
    factory = PGCraftLedger(
        "inventory",
        db_schema,
        metadata,
        schema_items=[
            Column("warehouse", String, nullable=False),
            Column("sku", String, nullable=False),
            Column("source", String),
            Column("reason", String),
        ],
        config=config,
    )
    PostgRESTView(
        source=factory,
        grants=["select", "insert", "update", "delete"],
    )
    create_all_from_metadata(db_conn, metadata)
    return db_conn, db_schema


# -------------------------------------------------------------------
# Simple event function helpers
# -------------------------------------------------------------------


def _create_simple_event_fn(  # noqa: PLR0913
    conn, schema, tablename, name, dim_keys, dim_types
):
    """Create a simple LANGUAGE sql event function."""
    view = f"api.{tablename}"
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
):
    """Create a simple event function with extra columns."""
    view = f"api.{tablename}"
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
):
    """Create a diff-mode LANGUAGE sql event function."""
    extra_cols = extra_cols or []
    extra_types = extra_types or []
    base = f"{schema}.{tablename}"
    view = f"api.{tablename}"

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

    desired_union_cols = ", ".join(all_cols_list)

    existing_union_parts = [
        *dim_keys,
        "value",
        *[f"NULL AS {c}" for c in extra_cols],
    ]
    existing_union_cols = ", ".join(existing_union_parts)

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
                f"SELECT SUM(value)"
                f" FROM {schema}.inventory"
                f" WHERE warehouse='NYC'"
                f" AND sku='A'"
            )
        ).scalar()
        assert total == 30


# -------------------------------------------------------------------
# Tests: diff event (reconciliation)
# -------------------------------------------------------------------


class TestDiffEvent:
    def test_reconciliation_inserts_correcting_deltas(self, inv):
        conn, schema = inv
        conn.execute(
            text(
                "INSERT INTO api.inventory"
                " (warehouse, sku, value)"
                " VALUES ('NYC', 'A', 50)"
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
        assert rows[0].value == 50

        balance = conn.execute(
            text(
                f"SELECT SUM(value)"
                f" FROM {schema}.inventory"
                f" WHERE warehouse='NYC'"
                f" AND sku='A'"
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
        conn.execute(
            text(
                "INSERT INTO api.inventory"
                " (warehouse, sku, value)"
                " VALUES ('LAX', 'B', 75)"
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
        assert len(rows) == 1
        assert rows[0].value == -75

        balance = conn.execute(
            text(
                f"SELECT SUM(value)"
                f" FROM {schema}.inventory"
                f" WHERE warehouse='LAX'"
                f" AND sku='B'"
            )
        ).scalar()
        assert balance == 0
