"""Plugins for simple (single-table) dimensions."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import Column, Table

if TYPE_CHECKING:
    from cave.factory.context import FactoryContext

from cave.plugin import Dynamic, Plugin, produces, requires, singleton
from cave.utils.template import load_template
from cave.utils.trigger import register_view_triggers

_TEMPLATES = Path(__file__).resolve().parent / "templates" / "simple"

_NAMING_DEFAULTS = {
    "simple_function": "%(schema)s_%(table_name)s_%(op)s",
    "simple_trigger": "%(schema)s_%(table_name)s_%(op)s",
}


def _dim_column_names(ctx: FactoryContext) -> list[str]:
    """Extract non-PK column names from schema_items."""
    return [
        col.key
        for col in ctx.schema_items
        if isinstance(col, Column) and not col.primary_key
    ]


@produces(Dynamic("table_key"))
@requires("pk_columns")
@singleton("__table__")
class SimpleTablePlugin(Plugin):
    """Create a single backing table for a simple dimension.

    Combines ``ctx["pk_columns"]`` and ``ctx.schema_items`` into one
    table.

    Args:
        table_key: Key under which the created table is stored in
            ``ctx`` (default ``"primary"``).

    """

    def __init__(self, table_key: str = "primary") -> None:
        """Store the context key."""
        self.table_key = table_key

    def run(self, ctx: FactoryContext) -> None:
        """Create the dimension table and store it in ctx."""
        pk_columns = ctx["pk_columns"]
        table = Table(
            ctx.tablename,
            ctx.metadata,
            *pk_columns,
            *ctx.schema_items,
            schema=ctx.schemaname,
        )
        ctx[self.table_key] = table


@requires(Dynamic("table_key"), Dynamic("view_key"))
class SimpleTriggerPlugin(Plugin):
    """Register INSERT/UPDATE/DELETE INSTEAD OF triggers on a view.

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
        """Register INSTEAD OF triggers on the target view."""
        api_view = ctx[self.view_key]
        primary = ctx[self.table_key]
        base_fullname = f"{ctx.schemaname}.{primary.name}"
        dim_cols = _dim_column_names(ctx)

        template_vars = {
            "base_table": base_fullname,
            "cols": ", ".join(dim_cols),
            "new_cols": ", ".join(f"NEW.{c}" for c in dim_cols),
            "set_clause": ", ".join(f"{c} = NEW.{c}" for c in dim_cols),
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
                (
                    "update",
                    load_template(_TEMPLATES / "update.mako"),
                ),
                (
                    "delete",
                    load_template(_TEMPLATES / "delete.mako"),
                ),
            ],
            naming_defaults=_NAMING_DEFAULTS,
            function_key="simple_function",
            trigger_key="simple_trigger",
        )
