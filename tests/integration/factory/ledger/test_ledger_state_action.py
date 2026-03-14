"""Integration tests for ledger StateAction functions.

Creates real database objects and exercises the begin/apply lifecycle
directly in SQL, independent of the full factory pipeline.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text

# ---------------------------------------------------------------------------
# Setup helpers
# ---------------------------------------------------------------------------


def _setup_pgcraft_utility(conn) -> None:
    """Create pgcraft schema and ledger_apply_state function."""
    conn.execute(text("CREATE SCHEMA IF NOT EXISTS pgcraft"))
    conn.execute(
        text("""
        CREATE OR REPLACE FUNCTION pgcraft.ledger_apply_state(
            p_target     TEXT,
            p_api_view   TEXT,
            p_staging    TEXT,
            p_diff_keys  TEXT[],
            p_write_cols TEXT[],
            p_write_vals TEXT[]
        ) RETURNS TABLE(delta_count BIGINT)
        LANGUAGE plpgsql SECURITY DEFINER AS $fn$
        DECLARE
            v_diff_col_list  TEXT;
            v_diff_s_col_list TEXT;
            v_join_cond      TEXT;
            v_group_by       TEXT;
            v_write_col_frag TEXT := '';
            v_write_val_frag TEXT := '';
            v_sql            TEXT;
            v_count          BIGINT;
            i                INT;
        BEGIN
            SELECT
                string_agg(quote_ident(k), ', ' ORDER BY ordinality),
                string_agg('s.' || quote_ident(k), ', ' ORDER BY ordinality),
                string_agg(
                    's.' || quote_ident(k) || ' = b.' || quote_ident(k),
                    ' AND ' ORDER BY ordinality
                ),
                string_agg(quote_ident(k), ', ' ORDER BY ordinality)
            INTO v_diff_col_list, v_diff_s_col_list, v_join_cond, v_group_by
            FROM unnest(p_diff_keys) WITH ORDINALITY AS u(k, ordinality);

            IF array_length(p_write_cols, 1) IS NOT NULL THEN
                FOR i IN 1..array_length(p_write_cols, 1) LOOP
                    v_write_col_frag := v_write_col_frag
                        || ', ' || quote_ident(p_write_cols[i]);
                    v_write_val_frag := v_write_val_frag
                        || ', '
                        || COALESCE(quote_literal(p_write_vals[i]), 'NULL');
                END LOOP;
            END IF;

            v_sql := format(
                'INSERT INTO %s (%s, value%s) '
                'SELECT %s, s.value - COALESCE(b.balance, 0)%s '
                'FROM %s s '
                'LEFT JOIN ('
                    'SELECT %s, SUM(value) AS balance '
                    'FROM %s '
                    'GROUP BY %s'
                ') b ON %s '
                'WHERE s.value - COALESCE(b.balance, 0) <> 0',
                p_api_view,
                v_diff_col_list, v_write_col_frag,
                v_diff_s_col_list, v_write_val_frag,
                p_staging,
                v_group_by, p_target, v_group_by,
                v_join_cond
            );
            EXECUTE v_sql;
            GET DIAGNOSTICS v_count = ROW_COUNT;
            RETURN QUERY SELECT v_count;
        END; $fn$
    """)
    )


def _setup_ledger(  # noqa: PLR0913
    conn,
    schema: str,
    tablename: str,
    diff_keys: list[str],
    diff_key_types: list[str],
    write_only_cols: list[str] | None = None,
    write_only_types: list[str] | None = None,
    partial: bool = True,  # noqa: FBT001, FBT002
) -> None:
    """Create table, API view, INSERT trigger, and begin/apply functions."""
    write_only_cols = write_only_cols or []
    write_only_types = write_only_types or []

    base = f"{schema}.{tablename}"
    view = f"{schema}.api_{tablename}"
    staging = f"_{tablename}_sync"

    dim_col_defs = "".join(
        f", {k} {t} NOT NULL"
        for k, t in zip(diff_keys, diff_key_types, strict=False)
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
        ["id", "entry_id", "created_at", "value", *diff_keys, *write_only_cols]
    )
    conn.execute(text(f"CREATE VIEW {view} AS SELECT {all_cols} FROM {base}"))

    # INSERT INSTEAD OF trigger.
    trig_ins_cols = ", ".join(
        ["entry_id", "value", *diff_keys, *write_only_cols]
    )
    trig_new_cols = ", ".join(
        [
            "COALESCE(NEW.entry_id, gen_random_uuid())",
            "NEW.value",
            *[f"NEW.{k}" for k in diff_keys],
            *[f"NEW.{k}" for k in write_only_cols],
        ]
    )
    conn.execute(
        text(f"""
        CREATE FUNCTION {schema}.{tablename}_ins()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            INSERT INTO {base} ({trig_ins_cols})
            VALUES ({trig_new_cols})
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

    # BEGIN function.
    staging_col_defs = ", ".join(
        [
            f"{k} {t} NOT NULL"
            for k, t in zip(diff_keys, diff_key_types, strict=False)
        ]
        + ["value INTEGER NOT NULL"]
    )
    conn.execute(
        text(f"""
        CREATE FUNCTION {schema}.{tablename}_sync_begin()
        RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS $$
        BEGIN
            CREATE TEMP TABLE IF NOT EXISTS {staging} (
                {staging_col_defs}
            ) ON COMMIT DELETE ROWS;
            TRUNCATE {staging};
        END; $$
    """)
    )

    # APPLY function.
    dk_array = "ARRAY[" + ", ".join(f"'{k}'" for k in diff_keys) + "]"
    wk_array = (
        "ARRAY[" + ", ".join(f"'{k}'" for k in write_only_cols) + "]::TEXT[]"
        if write_only_cols
        else "ARRAY[]::TEXT[]"
    )
    wp_array = (
        "ARRAY["
        + ", ".join(f"p_{k}::TEXT" for k in write_only_cols)
        + "]::TEXT[]"
        if write_only_cols
        else "ARRAY[]::TEXT[]"
    )

    wo_param_defs = "".join(
        f", p_{k} {t} DEFAULT NULL"
        for k, t in zip(write_only_cols, write_only_types, strict=False)
    )

    partial_false_sql = ""
    if not partial:
        insert_cols = ", ".join(diff_keys + write_only_cols + ["value"])
        select_diff = ", ".join(f"t.{k}" for k in diff_keys)
        wo_vals = (
            ", ".join(f"p_{k}" for k in write_only_cols) + ", "
            if write_only_cols
            else ""
        )
        diff_tuple = ", ".join(f"t.{k}" for k in diff_keys)
        diff_list = ", ".join(diff_keys)
        partial_false_sql = f"""
            INSERT INTO {view} ({insert_cols})
            SELECT {select_diff}, {wo_vals}-SUM(t.value)
            FROM {base} t
            WHERE ({diff_tuple}) NOT IN (
                SELECT {diff_list} FROM {staging}
            )
            GROUP BY {select_diff}
            HAVING SUM(t.value) <> 0;
        """

    conn.execute(
        text(f"""
        CREATE FUNCTION {schema}.{tablename}_sync_apply(
            {wo_param_defs.lstrip(", ")}
        )
        RETURNS TABLE(delta BIGINT)
        LANGUAGE plpgsql SECURITY DEFINER AS $$
        DECLARE v_delta BIGINT;
        BEGIN
            SELECT delta_count INTO v_delta
            FROM pgcraft.ledger_apply_state(
                '{base}', '{view}', '{staging}',
                {dk_array}, {wk_array}, {wp_array}
            );
            {partial_false_sql}
            TRUNCATE {staging};
            delta := v_delta;
            RETURN NEXT;
            RETURN;
        END; $$
    """)
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def inv(db_conn, db_schema):
    """Simple inventory ledger: diff_keys=[sku, warehouse]."""
    _setup_pgcraft_utility(db_conn)
    _setup_ledger(
        db_conn,
        db_schema,
        "inventory",
        diff_keys=["sku", "warehouse"],
        diff_key_types=["TEXT", "TEXT"],
    )
    return db_conn, db_schema


@pytest.fixture
def inv_full(db_conn, db_schema):
    """Inventory ledger with partial=False."""
    _setup_pgcraft_utility(db_conn)
    _setup_ledger(
        db_conn,
        db_schema,
        "inventory",
        diff_keys=["sku", "warehouse"],
        diff_key_types=["TEXT", "TEXT"],
        partial=False,
    )
    return db_conn, db_schema


@pytest.fixture
def inv_reason(db_conn, db_schema):
    """Inventory ledger with write-only 'reason' column."""
    _setup_pgcraft_utility(db_conn)
    _setup_ledger(
        db_conn,
        db_schema,
        "inventory",
        diff_keys=["sku", "warehouse"],
        diff_key_types=["TEXT", "TEXT"],
        write_only_cols=["reason"],
        write_only_types=["TEXT"],
    )
    return db_conn, db_schema


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _begin(conn, schema):
    conn.execute(text(f"SELECT {schema}.inventory_sync_begin()"))


def _staging_insert(conn, rows: list[tuple]) -> None:
    for sku, warehouse, value in rows:
        conn.execute(
            text(
                "INSERT INTO _inventory_sync (sku, warehouse, value) "
                "VALUES (:sku, :warehouse, :value)"
            ),
            {"sku": sku, "warehouse": warehouse, "value": value},
        )


def _apply(conn, schema, **kwargs) -> int:
    if not kwargs:
        return conn.execute(
            text(f"SELECT * FROM {schema}.inventory_sync_apply()")
        ).scalar()
    param_sql = ", ".join(f"p_{k} => :{k}" for k in kwargs)
    return conn.execute(
        text(f"SELECT * FROM {schema}.inventory_sync_apply({param_sql})"),
        kwargs,
    ).scalar()


def _balance(conn, schema, sku, warehouse) -> int:
    row = conn.execute(
        text(
            f"SELECT COALESCE(SUM(value), 0) FROM {schema}.inventory "
            "WHERE sku = :sku AND warehouse = :warehouse"
        ),
        {"sku": sku, "warehouse": warehouse},
    ).fetchone()
    return int(row[0])


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestStateActionBasic:
    def test_desired_matches_current_returns_zero(self, inv):
        conn, schema = inv
        conn.execute(
            text(
                f"INSERT INTO {schema}.api_inventory (sku, warehouse, value) "
                "VALUES ('A', 'NYC', 100)"
            )
        )
        _begin(conn, schema)
        _staging_insert(conn, [("A", "NYC", 100)])
        assert _apply(conn, schema) == 0

    def test_correcting_row_inserted_for_delta(self, inv):
        conn, schema = inv
        conn.execute(
            text(
                f"INSERT INTO {schema}.api_inventory (sku, warehouse, value) "
                "VALUES ('A', 'NYC', 50)"
            )
        )
        _begin(conn, schema)
        _staging_insert(conn, [("A", "NYC", 100)])
        assert _apply(conn, schema) == 1
        assert _balance(conn, schema, "A", "NYC") == 100

    def test_new_combo_inserts_positive_delta(self, inv):
        conn, schema = inv
        _begin(conn, schema)
        _staging_insert(conn, [("NEW", "DFW", 200)])
        assert _apply(conn, schema) == 1
        assert _balance(conn, schema, "NEW", "DFW") == 200

    def test_empty_staging_is_noop(self, inv):
        conn, schema = inv
        conn.execute(
            text(
                f"INSERT INTO {schema}.api_inventory (sku, warehouse, value) "
                "VALUES ('A', 'NYC', 100)"
            )
        )
        _begin(conn, schema)
        # No staging rows inserted.
        assert _apply(conn, schema) == 0
        assert _balance(conn, schema, "A", "NYC") == 100

    def test_other_groups_untouched_with_partial(self, inv):
        conn, schema = inv
        conn.execute(
            text(
                f"INSERT INTO {schema}.api_inventory (sku, warehouse, value) "
                "VALUES ('A', 'NYC', 30), ('B', 'LAX', 70)"
            )
        )
        _begin(conn, schema)
        _staging_insert(conn, [("A", "NYC", 50)])
        _apply(conn, schema)
        assert _balance(conn, schema, "B", "LAX") == 70

    def test_apply_is_idempotent(self, inv):
        conn, schema = inv
        _begin(conn, schema)
        _staging_insert(conn, [("A", "NYC", 100)])
        _apply(conn, schema)

        _begin(conn, schema)
        _staging_insert(conn, [("A", "NYC", 100)])
        assert _apply(conn, schema) == 0

    def test_staging_cleared_after_apply(self, inv):
        conn, schema = inv
        _begin(conn, schema)
        _staging_insert(conn, [("A", "NYC", 100)])
        _apply(conn, schema)

        count = conn.execute(
            text("SELECT COUNT(*) FROM _inventory_sync")
        ).scalar()
        assert count == 0


class TestStateActionPartialFalse:
    def test_absent_group_zeroed_out(self, inv_full):
        conn, schema = inv_full
        conn.execute(
            text(
                f"INSERT INTO {schema}.api_inventory (sku, warehouse, value) "
                "VALUES ('A', 'NYC', 50), ('B', 'LAX', 80)"
            )
        )
        _begin(conn, schema)
        # Only A in staging; B absent → zeroed.
        _staging_insert(conn, [("A", "NYC", 50)])
        _apply(conn, schema)
        assert _balance(conn, schema, "B", "LAX") == 0

    def test_groups_in_staging_reconciled(self, inv_full):
        conn, schema = inv_full
        conn.execute(
            text(
                f"INSERT INTO {schema}.api_inventory (sku, warehouse, value) "
                "VALUES ('A', 'NYC', 50)"
            )
        )
        _begin(conn, schema)
        _staging_insert(conn, [("A", "NYC", 100)])
        _apply(conn, schema)
        assert _balance(conn, schema, "A", "NYC") == 100


class TestStateActionWriteOnly:
    def test_write_only_written_on_inserted_row(self, inv_reason):
        conn, schema = inv_reason
        _begin(conn, schema)
        _staging_insert(conn, [("A", "NYC", 100)])
        _apply(conn, schema, reason="monthly_sync")

        row = conn.execute(
            text(
                f"SELECT reason FROM {schema}.inventory "
                "WHERE sku = 'A' ORDER BY id DESC LIMIT 1"
            )
        ).fetchone()
        assert row is not None
        assert row[0] == "monthly_sync"

    def test_write_only_null_when_omitted(self, inv_reason):
        conn, schema = inv_reason
        _begin(conn, schema)
        _staging_insert(conn, [("A", "NYC", 100)])
        _apply(conn, schema)  # no reason → NULL

        row = conn.execute(
            text(
                f"SELECT reason FROM {schema}.inventory "
                "WHERE sku = 'A' ORDER BY id DESC LIMIT 1"
            )
        ).fetchone()
        assert row is not None
        assert row[0] is None


class TestStateActionSessionIsolation:
    def test_staging_table_not_visible_before_begin(self, inv):
        """Staging table should not exist before _begin is called."""
        conn, schema = inv
        from sqlalchemy.exc import ProgrammingError

        with pytest.raises(ProgrammingError):
            conn.execute(text("SELECT COUNT(*) FROM _inventory_sync"))
