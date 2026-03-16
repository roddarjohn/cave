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
    Select,
    Table,
    Text,
    func,
    literal,
    select,
)
from sqlalchemy import cast as sa_cast
from sqlalchemy import types as sa_types

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
from pgcraft.plugins.trigger import TriggerOp
from pgcraft.plugins.view import ViewPlugin
from pgcraft.utils.naming import resolve_name
from pgcraft.utils.query import compile_query
from pgcraft.utils.template import load_template

_TEMPLATES = Path(__file__).resolve().parent / "templates" / "eav"

_NAMING_DEFAULTS = {
    "eav_entity": "%(table_name)s_entity",
    "eav_attribute": "%(table_name)s_attribute",
    "eav_function": "%(schema)s_%(table_name)s_%(op)s",
    "eav_trigger": "%(schema)s_%(table_name)s_%(op)s",
}


@dataclass
class _EAVMapping:
    """Internal mapping from a dimension name to EAV storage."""

    attribute_name: str
    value_column: str
    column_type: sa_types.TypeEngine
    nullable: bool = True


def _resolve_value_column(
    col: Column,
) -> tuple[str, sa_types.TypeEngine]:
    col_type = type(col.type)
    col_name = f"{col_type.__name__.lower()}_value"
    return col_name, col.type


def _build_eav_mappings(
    dimensions: list,
) -> list[_EAVMapping]:
    mappings: list[_EAVMapping] = []
    for dim in dimensions:
        if isinstance(dim, Column):
            value_col, col_type = _resolve_value_column(dim)
            mappings.append(
                _EAVMapping(
                    attribute_name=dim.key,
                    value_column=value_col,
                    column_type=col_type,
                    # dim.nullable is Optional[bool]; None means
                    # the Column was declared without an explicit
                    # nullable argument, which SQLAlchemy treats
                    # as nullable=True.
                    nullable=dim.nullable is not False,
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


def _pivot_aggregate(
    subquery: FromClause,
    mapping: _EAVMapping,
) -> Label:
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
    created_at_col: str = "created_at",
) -> Select:
    attr = attribute_table
    row_num = (
        func.row_number()
        .over(
            partition_by=[
                attr.c.entity_id,
                attr.c.attribute_name,
            ],
            order_by=[
                attr.c.created_at.desc(),
                attr.c.id.desc(),
            ],
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
            entity_table.c[created_at_col].label(created_at_col),
            *pivot_cols,
        )
        .select_from(
            entity_table.join(
                latest_current,
                latest_current.c.entity_id == entity_table.c.id,
                isouter=True,
            )
        )
        .group_by(
            entity_table.c.id,
            entity_table.c[created_at_col],
        )
    )


def _make_eav_query_builder(
    entity_key: str,
    attribute_key: str,
    mappings_key: str,
) -> Callable[[FactoryContext], str]:
    """Return a query builder for an EAV pivot view."""

    def build(ctx: FactoryContext) -> str:
        mappings: list[_EAVMapping] = ctx[mappings_key]
        entity_table = ctx[entity_key]
        attribute_table = ctx[attribute_key]
        created_at_col = ctx["created_at_column"]

        return compile_query(
            _build_pivot_query(
                entity_table,
                attribute_table,
                mappings,
                created_at_col,
            )
        )

    return build


def _make_eav_proxy_builder(
    mappings_key: str,
) -> Callable[[FactoryContext], list[Column]]:
    """Return a proxy builder for an EAV pivot view."""

    def build(ctx: FactoryContext) -> list[Column]:
        mappings: list[_EAVMapping] = ctx[mappings_key]
        pk_col_name = ctx.pk_column_name
        created_at_col = ctx["created_at_column"]
        return [
            Column(pk_col_name, Integer, primary_key=True),
            Column(created_at_col, DateTime(timezone=True)),
            *[Column(m.attribute_name, m.column_type) for m in mappings],
        ]

    return build


def _make_eav_ops_builder(
    entity_key: str,
    attribute_key: str,
    mappings_key: str,
) -> Callable[[FactoryContext], list[TriggerOp]]:
    """Return an ops builder for EAV dimensions."""

    def build(ctx: FactoryContext) -> list[TriggerOp]:
        mappings: list[_EAVMapping] = ctx[mappings_key]
        entity_table = ctx[entity_key]
        attribute_table = ctx[attribute_key]
        entity_fullname = f"{ctx.schemaname}.{entity_table.name}"
        attr_fullname = f"{ctx.schemaname}.{attribute_table.name}"

        template_vars = {
            "entity_table": entity_fullname,
            "attr_table": attr_fullname,
            "mappings": [
                (
                    m.attribute_name,
                    m.value_column,
                    m.nullable,
                )
                for m in mappings
            ],
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


@produces(
    Dynamic("entity_key"),
    Dynamic("attribute_key"),
    Dynamic("mappings_key"),
)
@requires("pk_columns", "created_at_column")
@singleton("__table__")
class EAVTablePlugin(Plugin):
    """Create entity and attribute tables for an EAV dimension.

    Args:
        entity_key: Key in ``ctx`` for the entity root table
            (default ``"entity"``).
        attribute_key: Key in ``ctx`` for the attribute log
            (default ``"attribute"``).
        mappings_key: Key in ``ctx`` for the EAV mappings list,
            shared with the view and trigger plugins
            (default ``"eav_mappings"``).

    """

    def __init__(
        self,
        entity_key: str = "entity",
        attribute_key: str = "attribute",
        mappings_key: str = "eav_mappings",
    ) -> None:
        """Store the context keys."""
        self.entity_key = entity_key
        self.attribute_key = attribute_key
        self.mappings_key = mappings_key

    def run(self, ctx: FactoryContext) -> None:
        """Create entity and attribute tables."""
        pk_col_name = ctx.pk_column_name
        created_at_col = ctx["created_at_column"]
        mappings = _build_eav_mappings(ctx.columns)
        ctx[self.mappings_key] = mappings

        entity_name = resolve_name(
            ctx.metadata,
            "eav_entity",
            {
                "table_name": ctx.tablename,
                "schema": ctx.schemaname,
            },
            _NAMING_DEFAULTS,
        )
        entity_table = Table(
            entity_name,
            ctx.metadata,
            Column(pk_col_name, Integer, primary_key=True),
            Column(
                created_at_col,
                DateTime(timezone=True),
                server_default="now()",
            ),
            schema=ctx.schemaname,
        )
        ctx[self.entity_key] = entity_table

        attr_name = resolve_name(
            ctx.metadata,
            "eav_attribute",
            {
                "table_name": ctx.tablename,
                "schema": ctx.schemaname,
            },
            _NAMING_DEFAULTS,
        )
        value_cols = _needed_value_columns(mappings)
        value_col_items = [
            Column(name, col_type, nullable=True)
            for name, col_type in value_cols.items()
        ]
        cols_list = ", ".join(value_cols)
        check = CheckConstraint(
            f"num_nonnulls({cols_list}) = 1",
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
        ctx[self.attribute_key] = attribute_table


def EAVViewPlugin(  # noqa: N802
    entity_key: str = "entity",
    attribute_key: str = "attribute",
    mappings_key: str = "eav_mappings",
    primary_key: str = "primary",
) -> ViewPlugin:
    """Create a configured ViewPlugin for EAV dimensions.

    Args:
        entity_key: Key in ``ctx`` for the entity root table
            (default ``"entity"``).
        attribute_key: Key in ``ctx`` for the attribute log
            (default ``"attribute"``).
        mappings_key: Key in ``ctx`` for the EAV mappings list
            (default ``"eav_mappings"``).
        primary_key: Key in ``ctx`` to store the view proxy
            under (default ``"primary"``).

    Returns:
        A :class:`~pgcraft.plugins.view.ViewPlugin` configured
        for EAV pivot views.

    """
    return ViewPlugin(
        query_builder=_make_eav_query_builder(
            entity_key, attribute_key, mappings_key
        ),
        proxy_builder=_make_eav_proxy_builder(mappings_key),
        primary_key=primary_key,
        extra_requires=[
            entity_key,
            attribute_key,
            mappings_key,
            "pk_columns",
            "created_at_column",
        ],
    )
