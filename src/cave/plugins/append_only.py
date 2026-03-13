"""Plugins for append-only (SCD Type 2) dimensions."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Integer,
    MetaData,
    Table,
    select,
)
from sqlalchemy_declarative_extensions import View, register_view

if TYPE_CHECKING:
    from cave.factory.context import FactoryContext

from cave.plugin import Dynamic, Plugin, produces, requires, singleton
from cave.utils.naming import resolve_name
from cave.utils.query import compile_query
from cave.utils.template import load_template
from cave.utils.trigger import register_view_triggers

_TEMPLATES = Path(__file__).resolve().parent / "templates" / "append_only"

_NAMING_DEFAULTS = {
    "append_only_root": "%(table_name)s_root",
    "append_only_attributes": "%(table_name)s_attributes",
    "append_only_function": "%(schema)s_%(table_name)s_%(op)s",
    "append_only_trigger": "%(schema)s_%(table_name)s_%(op)s",
}

_PK_COL = "id"  # always use "id" for internal FK references


def _resolve_root_name(ctx: FactoryContext) -> str:
    return resolve_name(
        ctx.metadata,
        "append_only_root",
        {"table_name": ctx.tablename, "schema": ctx.schemaname},
        _NAMING_DEFAULTS,
    )


def _resolve_attributes_name(ctx: FactoryContext) -> str:
    return resolve_name(
        ctx.metadata,
        "append_only_attributes",
        {"table_name": ctx.tablename, "schema": ctx.schemaname},
        _NAMING_DEFAULTS,
    )


def _dim_column_names(ctx: FactoryContext) -> list[str]:
    return [col.key for col in ctx.schema_items if isinstance(col, Column)]


@produces(Dynamic("root_key"), Dynamic("attributes_key"))
@singleton("__table__")
class AppendOnlyTablePlugin(Plugin):
    """Create the root and attributes tables for an append-only dimension.

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
        pk_col_name = ctx.pk_columns[0].key if ctx.pk_columns else _PK_COL

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
            *ctx.schema_items,
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
                "created_at",
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


@produces(Dynamic("primary_key"))
@requires(Dynamic("root_key"), Dynamic("attributes_key"))
class AppendOnlyViewPlugin(Plugin):
    """Create the join view for an append-only dimension.

    Args:
        root_key: Key in ``ctx`` for the entity root table
            (default ``"root_table"``).
        attributes_key: Key in ``ctx`` for the attributes log
            (default ``"attributes"``).
        primary_key: Key in ``ctx`` to store the view proxy under,
            for downstream plugins such as
            :class:`~cave.plugins.api.APIPlugin` (default
            ``"primary"``).

    """

    def __init__(
        self,
        root_key: str = "root_table",
        attributes_key: str = "attributes",
        primary_key: str = "primary",
    ) -> None:
        """Store the context keys."""
        self.root_key = root_key
        self.attributes_key = attributes_key
        self.primary_key = primary_key

    def run(self, ctx: FactoryContext) -> None:
        """Register the join view and store the proxy in ctx."""
        pk_col_name = ctx.pk_columns[0].key if ctx.pk_columns else _PK_COL
        root_table = ctx[self.root_key]
        attribute_table = ctx[self.attributes_key]

        view_query = (
            select(
                root_table.c[pk_col_name].label(pk_col_name),
                root_table.c["created_at"].label("created_at"),
                attribute_table.c["created_at"].label("updated_at"),
                *[
                    col.label(col.key)
                    for col in ctx.schema_items
                    if isinstance(col, Column)
                ],
            )
            .select_from(root_table)
            .join(
                attribute_table,
                attribute_table.c[pk_col_name]
                == root_table.c[f"{attribute_table.name}_id"],
            )
        )

        register_view(
            ctx.metadata,
            View(
                ctx.tablename,
                compile_query(view_query),
                schema=ctx.schemaname,
            ),
        )

        # Build a proxy Table so APIPlugin can SELECT from the view.
        proxy = Table(
            ctx.tablename,
            MetaData(),
            Column(pk_col_name, Integer, primary_key=True),
            Column("created_at", DateTime(timezone=True)),
            Column("updated_at", DateTime(timezone=True)),
            *[
                Column(col.key, col.type)
                for col in ctx.schema_items
                if isinstance(col, Column)
            ],
            schema=ctx.schemaname,
        )
        ctx[self.primary_key] = proxy


@requires(Dynamic("root_key"), Dynamic("attributes_key"), Dynamic("view_key"))
class AppendOnlyTriggerPlugin(Plugin):
    """Register INSTEAD OF triggers for an append-only dimension.

    Registers identical trigger logic on the private join view and,
    if present, the view stored at ``view_key``.

    Args:
        root_key: Key in ``ctx`` for the entity root table
            (default ``"root_table"``).
        attributes_key: Key in ``ctx`` for the attributes log
            (default ``"attributes"``).
        view_key: Key in ``ctx`` for the additional trigger target,
            e.g. the API view (default ``"api"``).  If the key is
            absent from ``ctx`` no second trigger is registered.

    """

    def __init__(
        self,
        root_key: str = "root_table",
        attributes_key: str = "attributes",
        view_key: str = "api",
    ) -> None:
        """Store the context keys."""
        self.root_key = root_key
        self.attributes_key = attributes_key
        self.view_key = view_key

    def run(self, ctx: FactoryContext) -> None:
        """Register INSTEAD OF triggers on the join view and API view."""
        root_table = ctx[self.root_key]
        attribute_table = ctx[self.attributes_key]
        root_fullname = f"{ctx.schemaname}.{root_table.name}"
        attr_fullname = f"{ctx.schemaname}.{attribute_table.name}"

        dim_cols = _dim_column_names(ctx)
        template_vars = {
            "attr_table": attr_fullname,
            "root_table": root_fullname,
            "attr_cols": ", ".join(dim_cols),
            "new_cols": ", ".join(f"NEW.{c}" for c in dim_cols),
            "attr_fk_col": f"{attribute_table.name}_id",
        }

        ops = [
            ("insert", load_template(_TEMPLATES / "insert.mako")),
            ("update", load_template(_TEMPLATES / "update.mako")),
            ("delete", load_template(_TEMPLATES / "delete.mako")),
        ]

        views_to_trigger = [
            (ctx.schemaname, f"{ctx.schemaname}.{ctx.tablename}"),
        ]
        if self.view_key in ctx:
            api_view = ctx[self.view_key]
            api_schema = api_view.schema or "api"
            views_to_trigger.append(
                (api_schema, f"{api_schema}.{ctx.tablename}")
            )

        for view_schema, view_fullname in views_to_trigger:
            register_view_triggers(
                metadata=ctx.metadata,
                view_schema=view_schema,
                view_fullname=view_fullname,
                tablename=ctx.tablename,
                template_vars=template_vars,
                ops=ops,
                naming_defaults=_NAMING_DEFAULTS,
                function_key="append_only_function",
                trigger_key="append_only_trigger",
            )
