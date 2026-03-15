"""Plugins for simple (single-table) dimensions."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import Table

if TYPE_CHECKING:
    from pgcraft.factory.context import FactoryContext

from pgcraft.plugin import Dynamic, Plugin, produces, requires, singleton
from pgcraft.utils.template import load_template
from pgcraft.utils.trigger import register_view_triggers

_TEMPLATES = Path(__file__).resolve().parent / "templates" / "simple"

_NAMING_DEFAULTS = {
    "simple_function": "%(schema)s_%(table_name)s_%(op)s",
    "simple_trigger": "%(schema)s_%(table_name)s_%(op)s",
}


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
        columns: list[str] | None = None,
        permitted_operations: list[str] | None = None,
    ) -> None:
        """Store the context keys.

        Args:
            table_key: Key in ``ctx`` for the backing table.
            view_key: Key in ``ctx`` for the API view.
            columns: Writable columns for the triggers.
                When ``None``, uses all dim columns from ctx.
            permitted_operations: DML operations to create
                INSTEAD OF triggers for (``"insert"``,
                ``"update"``, ``"delete"``).  When ``None``,
                creates all three.

        """
        self.table_key = table_key
        self.view_key = view_key
        self.columns = columns
        self.permitted_operations = permitted_operations

    def run(self, ctx: FactoryContext) -> None:
        """Register INSTEAD OF triggers on the target view."""
        api_view = ctx[self.view_key]
        primary = ctx[self.table_key]
        base_fullname = f"{ctx.schemaname}.{primary.name}"
        dim_cols = (
            self.columns if self.columns is not None else ctx.dim_column_names
        )

        # When column subset is active, RETURNING must list
        # only the view columns (PK + dim) instead of *.
        if self.columns is not None:
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

        api_schema = api_view.schema or "api"

        all_ops = [
            (
                "insert",
                load_template(_TEMPLATES / "insert.plpgsql.mako"),
            ),
            (
                "update",
                load_template(_TEMPLATES / "update.plpgsql.mako"),
            ),
            (
                "delete",
                load_template(_TEMPLATES / "delete.plpgsql.mako"),
            ),
        ]
        if self.permitted_operations is not None:
            grant_set = set(self.permitted_operations)
            all_ops = [(op, tmpl) for op, tmpl in all_ops if op in grant_set]

        if not all_ops:
            return

        register_view_triggers(
            metadata=ctx.metadata,
            view_schema=api_schema,
            view_fullname=f"{api_schema}.{ctx.tablename}",
            tablename=ctx.tablename,
            template_vars=template_vars,
            ops=all_ops,
            naming_defaults=_NAMING_DEFAULTS,
            function_key="simple_function",
            trigger_key="simple_trigger",
        )
