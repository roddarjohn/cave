"""Plugins for simple (single-table) dimensions."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import Table

if TYPE_CHECKING:
    from collections.abc import Callable

    from pgcraft.factory.context import FactoryContext

from pgcraft.plugin import Dynamic, Plugin, produces, requires, singleton
from pgcraft.plugins.trigger import InsteadOfTriggerPlugin, TriggerOp
from pgcraft.utils.template import load_template

_TEMPLATES = Path(__file__).resolve().parent / "templates" / "simple"

_NAMING_DEFAULTS = {
    "simple_function": "%(schema)s_%(table_name)s_%(op)s",
    "simple_trigger": "%(schema)s_%(table_name)s_%(op)s",
}


def _build_simple_ops_with_columns(
    columns: list[str] | None,
    table_key: str = "primary",
) -> Callable[[FactoryContext], list[TriggerOp]]:
    """Return an ops_builder that respects a column subset.

    Args:
        columns: Writable columns for the triggers.
            When ``None``, uses all dim columns from ctx.
        table_key: Key in ``ctx`` for the backing table.

    Returns:
        A callable suitable for ``InsteadOfTriggerPlugin``.

    """

    def builder(ctx: FactoryContext) -> list[TriggerOp]:
        primary = ctx[table_key]
        base_fullname = f"{ctx.schemaname}.{primary.name}"
        dim_cols = columns if columns is not None else ctx.dim_column_names

        if columns is not None:
            pk_name = ctx.pk_column_name
            ret = ", ".join([pk_name, *dim_cols])
        else:
            ret = "*"

        template_vars = {
            "base_table": base_fullname,
            "cols": ", ".join(dim_cols),
            "new_cols": ", ".join(f"NEW.{c}" for c in dim_cols),
            "set_clause": ", ".join(f"{c} = NEW.{c}" for c in dim_cols),
            "returning_cols": ret,
        }

        return [
            TriggerOp(
                "insert",
                load_template(_TEMPLATES / "insert.plpgsql.mako").render(
                    **template_vars
                ),
            ),
            TriggerOp(
                "update",
                load_template(_TEMPLATES / "update.plpgsql.mako").render(
                    **template_vars
                ),
            ),
            TriggerOp(
                "delete",
                load_template(_TEMPLATES / "delete.plpgsql.mako").render(
                    **template_vars
                ),
            ),
        ]

    return builder


@produces(Dynamic("table_key"), "__root__")
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
            *ctx.table_items,
            schema=ctx.schemaname,
        )
        ctx[self.table_key] = table
        ctx["__root__"] = table


class SimpleTriggerPlugin(InsteadOfTriggerPlugin):
    """Register INSERT/UPDATE/DELETE INSTEAD OF triggers on a view.

    Configured :class:`InsteadOfTriggerPlugin` for simple
    dimensions.

    Args:
        table_key: Key in ``ctx`` for the backing table
            (default ``"primary"``).
        view_key: Key in ``ctx`` for the trigger target view
            (default ``"api"``).
        columns: Writable columns for the triggers.
            When ``None``, uses all dim columns from ctx.
        permitted_operations: DML operations to create
            INSTEAD OF triggers for (``"insert"``,
            ``"update"``, ``"delete"``).  When ``None``,
            creates all three.

    """

    def __init__(
        self,
        table_key: str = "primary",
        view_key: str = "api",
        columns: list[str] | None = None,
        permitted_operations: list[str] | None = None,
    ) -> None:
        """Store the context keys."""
        self.table_key = table_key
        super().__init__(
            ops_builder=_build_simple_ops_with_columns(columns, table_key),
            naming_defaults=_NAMING_DEFAULTS,
            function_key="simple_function",
            trigger_key="simple_trigger",
            view_key=view_key,
            permitted_operations=permitted_operations,
            include_private_view=False,
        )
        self.columns = columns
