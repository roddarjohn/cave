"""Plugins for EAV (Entity-Attribute-Value) dimensions."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import (
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    FromClause,
    Integer,
    Label,
    MetaData,
    Select,
    Table,
    Text,
    func,
    literal,
    select,
)
from sqlalchemy import cast as sa_cast
from sqlalchemy import types as sa_types
from sqlalchemy_declarative_extensions import View, register_view

if TYPE_CHECKING:
    from cave.factory.context import FactoryContext

from cave.plugin import Plugin
from cave.utils.naming import resolve_name
from cave.utils.query import compile_query
from cave.utils.template import load_template
from cave.utils.trigger import register_view_triggers

_TEMPLATES = Path(__file__).resolve().parent / "templates" / "eav"

_NAMING_DEFAULTS = {
    "eav_entity": "%(table_name)s_entity",
    "eav_attribute": "%(table_name)s_attribute",
    "eav_function": "%(schema)s_%(table_name)s_%(op)s",
    "eav_trigger": "%(schema)s_%(table_name)s_%(op)s",
}

_PK_COL = "id"
_STATE_MAPPINGS = "eav_mappings"


@dataclass
class _EAVMapping:
    """Internal mapping from a dimension name to EAV storage."""

    attribute_name: str
    value_column: str
    column_type: sa_types.TypeEngine


def _resolve_value_column(col: Column) -> tuple[str, sa_types.TypeEngine]:
    col_type = type(col.type)
    col_name = f"{col_type.__name__.lower()}_value"
    return col_name, col.type


def _build_eav_mappings(dimensions: list) -> list[_EAVMapping]:
    mappings: list[_EAVMapping] = []
    for dim in dimensions:
        if isinstance(dim, Column):
            value_col, col_type = _resolve_value_column(dim)
            mappings.append(
                _EAVMapping(
                    attribute_name=dim.key,
                    value_column=value_col,
                    column_type=col_type,
                )
            )
    return mappings


def _needed_value_columns(
    mappings: list[_EAVMapping],
) -> dict[str, sa_types.TypeEngine]:
    cols: dict[str, sa_types.TypeEngine] = {}
    for mapping in mappings:
        if mapping.value_column not in cols:
            cols[mapping.value_column] = mapping.column_type
    return cols


def _pivot_aggregate(subquery: FromClause, mapping: _EAVMapping) -> Label:
    col = subquery.c[mapping.value_column]
    condition = subquery.c.attribute_name == literal(mapping.attribute_name)
    if isinstance(mapping.column_type, sa_types.Boolean):
        return sa_cast(
            func.max(sa_cast(col, Integer)).filter(condition),
            sa_types.Boolean(),
        ).label(mapping.attribute_name)
    return func.max(col).filter(condition).label(mapping.attribute_name)


def _build_pivot_query(
    entity_table: Table,
    attribute_table: Table,
    mappings: list[_EAVMapping],
) -> Select:
    attr = attribute_table
    row_num = (
        func.row_number()
        .over(
            partition_by=[attr.c.entity_id, attr.c.attribute_name],
            order_by=[attr.c.created_at.desc(), attr.c.id.desc()],
        )
        .label("rn")
    )
    latest = (
        select(attr, row_num)
        .where(attr.c.attribute_name.in_([m.attribute_name for m in mappings]))
        .cte("latest")
    )
    latest_current = select(latest).where(latest.c.rn == 1).subquery("cur")
    pivot_cols = [_pivot_aggregate(latest_current, m) for m in mappings]
    return (
        select(
            entity_table.c.id.label("id"),
            entity_table.c.created_at.label("created_at"),
            *pivot_cols,
        )
        .select_from(
            entity_table.join(
                latest_current,
                latest_current.c.entity_id == entity_table.c.id,
                isouter=True,
            )
        )
        .group_by(entity_table.c.id, entity_table.c.created_at)
    )


class EAVTablePlugin(Plugin):
    """Create the entity and attribute tables for an EAV dimension.

    Sets:
        - ``ctx.tables["entity"]`` -- the entity root table.
        - ``ctx.tables["attribute"]`` -- the typed attribute table.
        - ``ctx.state["eav_mappings"]`` -- list of ``_EAVMapping``.
    """

    def create_tables(self, ctx: FactoryContext) -> None:
        """Create entity and attribute tables."""
        pk_col_name = ctx.pk_columns[0].key if ctx.pk_columns else _PK_COL
        mappings = _build_eav_mappings(ctx.dimensions)
        ctx.state[_STATE_MAPPINGS] = mappings

        entity_name = resolve_name(
            ctx.metadata,
            "eav_entity",
            {"table_name": ctx.tablename, "schema": ctx.schemaname},
            _NAMING_DEFAULTS,
        )
        entity_table = Table(
            entity_name,
            ctx.metadata,
            Column(pk_col_name, Integer, primary_key=True),
            Column(
                "created_at",
                DateTime(timezone=True),
                server_default="now()",
            ),
            schema=ctx.schemaname,
        )
        ctx.tables["entity"] = entity_table

        attr_name = resolve_name(
            ctx.metadata,
            "eav_attribute",
            {"table_name": ctx.tablename, "schema": ctx.schemaname},
            _NAMING_DEFAULTS,
        )
        value_cols = _needed_value_columns(mappings)
        value_col_items = [
            Column(name, col_type, nullable=True)
            for name, col_type in value_cols.items()
        ]
        check_expr = " + ".join(f"({vc} IS NOT NULL)::int" for vc in value_cols)
        check = CheckConstraint(
            f"{check_expr} = 1",
            name=f"{attr_name}_one_value_ck",
        )
        entity_fq = f"{ctx.schemaname}.{entity_name}.{pk_col_name}"
        attribute_table = Table(
            attr_name,
            ctx.metadata,
            Column(pk_col_name, Integer, primary_key=True),
            Column(
                "entity_id",
                ForeignKey(entity_fq, ondelete="CASCADE"),
                nullable=False,
            ),
            Column("attribute_name", Text, nullable=False),
            *value_col_items,
            Column(
                "created_at",
                DateTime(timezone=True),
                server_default="now()",
            ),
            check,
            schema=ctx.schemaname,
        )
        ctx.tables["attribute"] = attribute_table


class EAVViewPlugin(Plugin):
    """Create the pivot view for an EAV dimension.

    Reads ``ctx.tables["entity"]``, ``ctx.tables["attribute"]``, and
    ``ctx.state["eav_mappings"]``.  Sets ``ctx.tables["primary"]`` to
    a view proxy for use by :class:`~cave.plugins.api.APIPlugin`.
    """

    def create_views(self, ctx: FactoryContext) -> None:
        """Register the pivot view and set ctx.tables["primary"]."""
        mappings: list[_EAVMapping] = ctx.state[_STATE_MAPPINGS]
        entity_table = ctx.tables["entity"]
        attribute_table = ctx.tables["attribute"]
        pk_col_name = ctx.pk_columns[0].key if ctx.pk_columns else _PK_COL

        view_sql = compile_query(
            _build_pivot_query(entity_table, attribute_table, mappings)
        )
        register_view(
            ctx.metadata,
            View(ctx.tablename, view_sql, schema=ctx.schemaname),
        )

        proxy = Table(
            ctx.tablename,
            MetaData(),
            Column(pk_col_name, Integer, primary_key=True),
            Column("created_at", DateTime(timezone=True)),
            *[Column(m.attribute_name, m.column_type) for m in mappings],
            schema=ctx.schemaname,
        )
        ctx.tables["primary"] = proxy


class EAVTriggerPlugin(Plugin):
    """Register INSTEAD OF triggers for an EAV dimension.

    Registers on the private pivot view and, if present, the API view.
    """

    def create_triggers(self, ctx: FactoryContext) -> None:
        """Register INSTEAD OF triggers on both views."""
        mappings: list[_EAVMapping] = ctx.state[_STATE_MAPPINGS]
        entity_table = ctx.tables["entity"]
        attribute_table = ctx.tables["attribute"]
        entity_fullname = f"{ctx.schemaname}.{entity_table.name}"
        attr_fullname = f"{ctx.schemaname}.{attribute_table.name}"

        template_vars = {
            "entity_table": entity_fullname,
            "attr_table": attr_fullname,
            "mappings": [(m.attribute_name, m.value_column) for m in mappings],
        }

        ops = [
            ("insert", load_template(_TEMPLATES / "insert.mako")),
            ("update", load_template(_TEMPLATES / "update.mako")),
            ("delete", load_template(_TEMPLATES / "delete.mako")),
        ]

        views_to_trigger = [
            (ctx.schemaname, f"{ctx.schemaname}.{ctx.tablename}"),
        ]
        if "api" in ctx.views:
            api_view = ctx.views["api"]
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
                function_key="eav_function",
                trigger_key="eav_trigger",
            )
