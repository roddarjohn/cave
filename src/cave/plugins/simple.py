"""Plugins for simple (single-table) dimensions."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import Column, Table

if TYPE_CHECKING:
    from cave.factory.context import FactoryContext

from cave.plugin import Plugin
from cave.utils.template import load_template
from cave.utils.trigger import register_view_triggers

_TEMPLATES = Path(__file__).resolve().parent / "templates" / "simple"

_NAMING_DEFAULTS = {
    "simple_function": "%(schema)s_%(table_name)s_%(op)s",
    "simple_trigger": "%(schema)s_%(table_name)s_%(op)s",
}


def _dim_column_names(ctx: FactoryContext) -> list[str]:
    """Extract non-PK column names from dimensions."""
    return [
        col.key
        for col in ctx.dimensions
        if isinstance(col, Column) and not col.primary_key
    ]


class SimpleTablePlugin(Plugin):
    """Create a single backing table for a simple dimension.

    Combines ``ctx.pk_columns``, ``ctx.extra_columns``, and
    ``ctx.dimensions`` into one table.  Sets
    ``ctx.tables["primary"]`` to the created table.
    """

    def create_tables(self, ctx: FactoryContext) -> None:
        """Create the dimension table and set it as primary."""
        table = Table(
            ctx.tablename,
            ctx.metadata,
            *ctx.pk_columns,
            *ctx.extra_columns,
            *ctx.dimensions,
            schema=ctx.schemaname,
        )
        ctx.tables["primary"] = table


class SimpleTriggerPlugin(Plugin):
    """Register INSERT/UPDATE/DELETE INSTEAD OF triggers on the API view.

    Reads ``ctx.views["api"]`` for the target view and
    ``ctx.tables["primary"]`` for the backing table.
    """

    def create_triggers(self, ctx: FactoryContext) -> None:
        """Register INSTEAD OF triggers on the API view."""
        api_view = ctx.views["api"]
        primary = ctx.tables["primary"]
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
                ("insert", load_template(_TEMPLATES / "insert.mako")),
                ("update", load_template(_TEMPLATES / "update.mako")),
                ("delete", load_template(_TEMPLATES / "delete.mako")),
            ],
            naming_defaults=_NAMING_DEFAULTS,
            function_key="simple_function",
            trigger_key="simple_trigger",
        )
