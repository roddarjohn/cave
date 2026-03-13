"""Plugins for ledger (append-only value) tables."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import Column, DateTime, Integer, Numeric, Table

if TYPE_CHECKING:
    from pgcraft.factory.context import FactoryContext

from pgcraft.errors import PGCraftValidationError
from pgcraft.plugin import Dynamic, Plugin, produces, requires, singleton
from pgcraft.utils.template import load_template
from pgcraft.utils.trigger import register_view_triggers

_TEMPLATES = Path(__file__).resolve().parent / "templates" / "ledger"

_NAMING_DEFAULTS = {
    "ledger_function": "%(schema)s_%(table_name)s_%(op)s",
    "ledger_trigger": "%(schema)s_%(table_name)s_%(op)s",
}

_VALUE_TYPES = {
    "integer": Integer,
    "numeric": Numeric,
}


def _dim_column_names(ctx: FactoryContext) -> list[str]:
    """Extract writable (non-PK, non-computed) column names."""
    return [
        col.key
        for col in ctx.columns
        if not col.primary_key and not col.computed
    ]


@produces(Dynamic("table_key"), "__root__")
@requires("pk_columns", "entry_id_column", "created_at_column")
@singleton("__table__")
class LedgerTablePlugin(Plugin):
    """Create a ledger table with a value column.

    Combines ``ctx["pk_columns"]``, ``ctx["entry_id_column"]``,
    a ``created_at`` timestamp, a ``value`` column, and
    ``ctx.table_items`` (dimension columns) into a single
    append-only table.

    Args:
        value_type: Type for the value column. Must be
            ``"integer"`` or ``"numeric"`` (default ``"integer"``).
        table_key: Key under which the created table is stored
            in ``ctx`` (default ``"primary"``).

    Raises:
        PGCraftValidationError: If *value_type* is not a
            recognised type.

    """

    def __init__(
        self,
        value_type: str = "integer",
        table_key: str = "primary",
    ) -> None:
        """Store configuration."""
        if value_type not in _VALUE_TYPES:
            msg = (
                f"Unknown value_type {value_type!r}. "
                f"Must be one of: {sorted(_VALUE_TYPES)}"
            )
            raise PGCraftValidationError(msg)
        self.value_type = value_type
        self.table_key = table_key

    def run(self, ctx: FactoryContext) -> None:
        """Create the ledger table and store it in ctx."""
        pk_columns = ctx["pk_columns"]
        entry_id_col = ctx["entry_id_column"]
        created_at_col = ctx["created_at_column"]
        sa_type = _VALUE_TYPES[self.value_type]

        table = Table(
            ctx.tablename,
            ctx.metadata,
            *pk_columns,
            entry_id_col,
            Column(
                created_at_col,
                DateTime(timezone=True),
                server_default="now()",
            ),
            Column("value", sa_type(), nullable=False),
            *ctx.table_items,
            schema=ctx.schemaname,
        )
        ctx[self.table_key] = table
        ctx["__root__"] = table


@requires(Dynamic("table_key"), Dynamic("view_key"), "entry_id_column")
class LedgerTriggerPlugin(Plugin):
    """Register an INSERT INSTEAD OF trigger on a ledger view.

    Only INSERT is supported -- ledger entries are immutable.
    UPDATE and DELETE on the API view will raise a PostgreSQL
    error naturally (no INSTEAD OF trigger defined).

    Args:
        table_key: Key in ``ctx`` for the backing table
            (default ``"primary"``).
        view_key: Key in ``ctx`` for the trigger target view
            (default ``"api"``).

    """

    def __init__(
        self,
        table_key: str = "primary",
        view_key: str = "api",
    ) -> None:
        """Store the context keys."""
        self.table_key = table_key
        self.view_key = view_key

    def run(self, ctx: FactoryContext) -> None:
        """Register INSTEAD OF INSERT trigger on the API view."""
        api_view = ctx[self.view_key]
        primary = ctx[self.table_key]
        base_fullname = f"{ctx.schemaname}.{primary.name}"
        entry_id_col = ctx["entry_id_column"]
        dim_cols = _dim_column_names(ctx)

        # Include entry_id and value alongside dimension columns.
        all_cols = [entry_id_col.name, "value", *dim_cols]
        template_vars = {
            "base_table": base_fullname,
            "cols": ", ".join(all_cols),
            "new_cols": ", ".join(f"NEW.{c}" for c in all_cols),
        }

        api_schema = api_view.schema or "api"
        register_view_triggers(
            metadata=ctx.metadata,
            view_schema=api_schema,
            view_fullname=f"{api_schema}.{ctx.tablename}",
            tablename=ctx.tablename,
            template_vars=template_vars,
            ops=[
                (
                    "insert",
                    load_template(_TEMPLATES / "insert.mako"),
                ),
            ],
            naming_defaults=_NAMING_DEFAULTS,
            function_key="ledger_function",
            trigger_key="ledger_trigger",
        )
