"""pgcraft utility schema registration.

Registers the ``pgcraft`` schema (or a configurable alternative) and
the ``ledger_apply_state`` generic diff-and-insert function on a
SQLAlchemy :class:`~sqlalchemy.MetaData` instance.

The registration is idempotent: calling :func:`_ensure_pgcraft_utilities`
multiple times on the same metadata object is safe and cheap.
"""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

from sqlalchemy_declarative_extensions import Schema, Schemas, register_function
from sqlalchemy_declarative_extensions.dialects.postgresql import (
    Function,
    FunctionParam,
    FunctionSecurity,
)

if TYPE_CHECKING:
    from sqlalchemy import MetaData

# ---------------------------------------------------------------------------
# ledger_apply_state body
# ---------------------------------------------------------------------------
# Computes current balances for each diff-key group in p_target, then
# inserts correcting rows into p_api_view for every group whose desired
# value (from staging) differs from the current balance.  Write-only
# column values are injected verbatim as SQL literals.
# ---------------------------------------------------------------------------

_LEDGER_APPLY_STATE_BODY = """\
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
                || ', ' || COALESCE(quote_literal(p_write_vals[i]), 'NULL');
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
END;"""


def _ensure_pgcraft_utilities(
    metadata: MetaData,
    schema: str = "pgcraft",
) -> None:
    """Register the pgcraft utility schema and functions on *metadata*.

    Idempotent: subsequent calls with the same *metadata* and *schema*
    are no-ops (checked via ``metadata.info``).

    Registers:

    - The *schema* itself (so Alembic emits ``CREATE SCHEMA``).
    - ``{schema}.ledger_apply_state`` -- generic diff-and-insert
      engine used by all :class:`~pgcraft.ledger.actions.StateAction`
      apply functions.

    Args:
        metadata: SQLAlchemy ``MetaData`` to register objects on.
        schema: Utility schema name (default ``"pgcraft"``).

    """
    key = f"_pgcraft_utilities_registered:{schema}"
    if metadata.info.get(key):
        return

    # Register the schema so Alembic creates it.
    existing: Schemas | None = metadata.info.get("schemas")
    if existing is None:
        metadata.info["schemas"] = Schemas(ignore_unspecified=True).are(schema)
    else:
        already = {s.name for s in existing.schemas}
        if schema not in already:
            metadata.info["schemas"] = replace(
                existing,
                schemas=(*existing.schemas, Schema(schema)),
            )

    # Register ledger_apply_state.
    register_function(
        metadata,
        Function(
            "ledger_apply_state",
            _LEDGER_APPLY_STATE_BODY,
            returns="TABLE(delta_count BIGINT)",
            language="plpgsql",
            schema=schema,
            parameters=[
                FunctionParam.input("p_target", "TEXT"),
                FunctionParam.input("p_api_view", "TEXT"),
                FunctionParam.input("p_staging", "TEXT"),
                FunctionParam.input("p_diff_keys", "TEXT[]"),
                FunctionParam.input("p_write_cols", "TEXT[]"),
                FunctionParam.input("p_write_vals", "TEXT[]"),
                FunctionParam.table("delta_count", "BIGINT"),
            ],
            security=FunctionSecurity.definer,
        ),
    )

    metadata.info[key] = True
