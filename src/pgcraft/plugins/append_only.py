"""Plugins for append-only (SCD Type 2) dimensions."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Integer,
    Table,
    select,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from pgcraft.factory.context import FactoryContext

from pgcraft.plugin import (
    Dynamic,
    Plugin,
    produces,
    requires,
    singleton,
)
from pgcraft.plugins.trigger import (
    InsteadOfTriggerPlugin,
    TriggerOp,
)
from pgcraft.plugins.view import ViewPlugin
from pgcraft.utils.naming import resolve_name
from pgcraft.utils.query import compile_query
from pgcraft.utils.template import load_template

_TEMPLATES = Path(__file__).resolve().parent / "templates" / "append_only"

_NAMING_DEFAULTS = {
    "append_only_root": "%(table_name)s_root",
    "append_only_attributes": "%(table_name)s_attributes",
    "append_only_function": ("%(schema)s_%(table_name)s_%(op)s"),
    "append_only_trigger": ("%(schema)s_%(table_name)s_%(op)s"),
}


def _resolve_root_name(ctx: FactoryContext) -> str:
    return resolve_name(
        ctx.metadata,
        "append_only_root",
        {
            "table_name": ctx.tablename,
            "schema": ctx.schemaname,
        },
        _NAMING_DEFAULTS,
    )


def _resolve_attributes_name(
    ctx: FactoryContext,
) -> str:
    return resolve_name(
        ctx.metadata,
        "append_only_attributes",
        {
            "table_name": ctx.tablename,
            "schema": ctx.schemaname,
        },
        _NAMING_DEFAULTS,
    )


# -- builder factories ------------------------------------------------


def _make_query_builder(
    root_key: str,
    attributes_key: str,
) -> Callable[[FactoryContext], str]:
    """Return a query builder for an append-only join view."""

    def build(ctx: FactoryContext) -> str:
        pk_col_name = ctx.pk_column_name
        created_at_col = ctx["created_at_column"]
        root_table = ctx[root_key]
        attribute_table = ctx[attributes_key]

        view_query = (
            select(
                root_table.c[pk_col_name].label(pk_col_name),
                root_table.c[created_at_col].label(created_at_col),
                attribute_table.c["created_at"].label("updated_at"),
                *[col.label(col.key) for col in ctx.columns],
            )
            .select_from(root_table)
            .join(
                attribute_table,
                attribute_table.c[pk_col_name]
                == root_table.c[f"{attribute_table.name}_id"],
            )
        )
        return compile_query(view_query)

    return build


def _build_proxy(ctx: FactoryContext) -> list[Column]:
    """Build proxy columns for an append-only join view."""
    pk_col_name = ctx.pk_column_name
    created_at_col = ctx["created_at_column"]
    return [
        Column(pk_col_name, Integer, primary_key=True),
        Column(created_at_col, DateTime(timezone=True)),
        Column("updated_at", DateTime(timezone=True)),
        *[Column(col.key, col.type) for col in ctx.columns],
    ]


def _make_ops_builder(
    root_key: str,
    attributes_key: str,
    columns: list[str] | None = None,
) -> Callable[[FactoryContext], list[TriggerOp]]:
    """Return an ops builder for append-only dimensions."""

    def build(ctx: FactoryContext) -> list[TriggerOp]:
        root_table = ctx[root_key]
        attribute_table = ctx[attributes_key]
        root_fullname = f"{ctx.schemaname}.{root_table.name}"
        attr_fullname = f"{ctx.schemaname}.{attribute_table.name}"

        if columns is not None:
            dim_cols = columns
        elif "writable_columns" in ctx:
            dim_cols = ctx["writable_columns"]
        else:
            dim_cols = ctx.dim_column_names
        template_vars = {
            "attr_table": attr_fullname,
            "root_table": root_fullname,
            "attr_cols": ", ".join(dim_cols),
            "new_cols": ", ".join(f"NEW.{c}" for c in dim_cols),
            "attr_fk_col": f"{attribute_table.name}_id",
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

    return build


# -- plugins -----------------------------------------------------------


@produces(Dynamic("root_key"), Dynamic("attributes_key"))
@requires("pk_columns", "created_at_column")
@singleton("__table__")
class AppendOnlyTablePlugin(Plugin):
    """Create the root and attributes tables for an append-only dim.

    Args:
        root_key: Key in ``ctx`` for the entity root table
            (default ``"root_table"``).
        attributes_key: Key in ``ctx`` for the append-only
            attributes log (default ``"attributes"``).

    """

    def __init__(
        self,
        root_key: str = "root_table",
        attributes_key: str = "attributes",
    ) -> None:
        """Store the context keys."""
        self.root_key = root_key
        self.attributes_key = attributes_key

    def run(self, ctx: FactoryContext) -> None:
        """Create root and attributes tables."""
        pk_col_name = ctx.pk_column_name
        created_at_col = ctx["created_at_column"]

        attr_name = _resolve_attributes_name(ctx)
        attributes_table = Table(
            attr_name,
            ctx.metadata,
            Column(pk_col_name, Integer, primary_key=True),
            Column(
                "created_at",
                DateTime(timezone=True),
                server_default="now()",
            ),
            *ctx.table_items,
            schema=ctx.schemaname,
        )
        ctx[self.attributes_key] = attributes_table

        root_name = _resolve_root_name(ctx)
        root_fk = f"{ctx.schemaname}.{attr_name}.{pk_col_name}"
        root_table = Table(
            root_name,
            ctx.metadata,
            Column(pk_col_name, Integer, primary_key=True),
            Column(
                created_at_col,
                DateTime(timezone=True),
                server_default="now()",
            ),
            Column(
                f"{attr_name}_id",
                ForeignKey(root_fk),
            ),
            schema=ctx.schemaname,
        )
        ctx[self.root_key] = root_table


# -- factory functions (replace former plugin classes) -----------------


def AppendOnlyViewPlugin(  # noqa: N802
    root_key: str = "root_table",
    attributes_key: str = "attributes",
    primary_key: str = "primary",
) -> ViewPlugin:
    """Create a configured ViewPlugin for append-only dimensions.

    Args:
        root_key: Key in ``ctx`` for the entity root table
            (default ``"root_table"``).
        attributes_key: Key in ``ctx`` for the attributes log
            (default ``"attributes"``).
        primary_key: Key in ``ctx`` to store the view proxy
            under (default ``"primary"``).

    Returns:
        A :class:`~pgcraft.plugins.view.ViewPlugin` configured
        for append-only join views.

    """
    return ViewPlugin(
        query_builder=_make_query_builder(root_key, attributes_key),
        proxy_builder=_build_proxy,
        primary_key=primary_key,
        extra_requires=[
            root_key,
            attributes_key,
            "pk_columns",
            "created_at_column",
        ],
    )


def append_only_trigger_plugin(
    columns: list[str] | None = None,
    root_key: str = "root_table",
    attributes_key: str = "attributes",
    view_key: str = "api",
) -> InsteadOfTriggerPlugin:
    """Create a configured InsteadOfTriggerPlugin for append-only.

    Args:
        root_key: Key in ``ctx`` for the root table
            (default ``"root_table"``).
        attributes_key: Key in ``ctx`` for the attribute
            table (default ``"attributes"``).
        view_key: Key in ``ctx`` for the API view
            (default ``"api"``).
        columns: Writable columns for the triggers.
            When ``None``, uses all dim columns from ctx.

    Returns:
        A configured
        :class:`~pgcraft.plugins.trigger.InsteadOfTriggerPlugin`.

    """
    return InsteadOfTriggerPlugin(
        ops_builder=_make_ops_builder(root_key, attributes_key, columns),
        naming_defaults=_NAMING_DEFAULTS,
        function_key="append_only_function",
        trigger_key="append_only_trigger",
        view_key=view_key,
        extra_requires=[root_key, attributes_key],
    )
