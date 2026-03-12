"""EAV (Entity-Attribute-Value) dimension factory.

Creates an entity table and an append-only typed attribute table
with a CHECK constraint enforcing exactly one value column per
row.  Multiple rows per ``(entity_id, attribute_name)`` are
allowed, providing full audit history.  A pivot view reconstructs
the current columnar representation using the latest attribute
row per entity.
"""

from dataclasses import dataclass

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
from sqlalchemy import (
    types as sa_types,
)
from sqlalchemy.schema import SchemaItem
from sqlalchemy_declarative_extensions import (
    View,
    register_view,
)

from cave.factory.dimension.base import DimensionFactory, FactoryContext
from cave.utils.naming import resolve_name
from cave.utils.query import compile_query
from cave.utils.template import load_template
from cave.utils.trigger import register_view_triggers

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


def _resolve_value_column(
    col: Column,
) -> tuple[str, sa_types.TypeEngine]:
    """Derive the EAV value column from a Column's type.

    The column name is ``{type_class_name.lower()}_value``
    and the storage type is the column's own type instance.
    Any SQLAlchemy type is supported automatically.
    """
    col_type = type(col.type)
    col_name = f"{col_type.__name__.lower()}_value"
    return col_name, col.type


def _build_eav_mappings(
    dimensions: list[SchemaItem],
) -> list[_EAVMapping]:
    """Build EAV mappings for every Column in *dimensions*."""
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
    """Return deduplicated value columns required by *mappings*."""
    cols: dict[str, sa_types.TypeEngine] = {}
    for mapping in mappings:
        if mapping.value_column not in cols:
            cols[mapping.value_column] = mapping.column_type
    return cols


def _construct_entity_table(
    tablename: str,
    schemaname: str,
    metadata: MetaData,
    config_id: str,
) -> Table:
    """Create the ``{tablename}_entity`` table."""
    entity_tablename = resolve_name(
        metadata,
        "eav_entity",
        {"table_name": tablename, "schema": schemaname},
        _NAMING_DEFAULTS,
    )
    return Table(
        entity_tablename,
        metadata,
        Column(config_id, Integer, primary_key=True),
        Column(
            "created_at",
            DateTime(timezone=True),
            server_default="now()",
        ),
        schema=schemaname,
    )


def _construct_attribute_table(  # noqa: PLR0913
    tablename: str,
    schemaname: str,
    metadata: MetaData,
    config_id: str,
    entity_table: Table,
    mappings: list[_EAVMapping],
) -> Table:
    """Create the ``{tablename}_attribute`` table.

    Includes only the value columns actually used and a CHECK
    constraint enforcing exactly one non-null value column.
    No unique constraint -- multiple rows per
    ``(entity_id, attribute_name)`` provide full audit history.
    """
    attr_tablename = resolve_name(
        metadata,
        "eav_attribute",
        {"table_name": tablename, "schema": schemaname},
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
        name=f"{attr_tablename}_one_value_ck",
    )

    entity_fq = f"{schemaname}.{entity_table.name}.{config_id}"

    return Table(
        attr_tablename,
        metadata,
        Column(config_id, Integer, primary_key=True),
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
        schema=schemaname,
    )


def _pivot_aggregate(
    subquery: FromClause,
    mapping: _EAVMapping,
) -> Label:
    """Build a MAX(...) FILTER (...) pivot expression.

    PostgreSQL lacks ``MAX(boolean)``, so boolean columns are
    cast to integer for aggregation, then back to boolean.
    """
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
    """Build the pivot view query from EAV mappings.

    Uses a CTE with ``ROW_NUMBER()`` to find the latest row
    per ``(entity_id, attribute_name)``, scanning the attribute
    table once.  The outer query joins the entity table to the
    CTE and pivots via ``MAX(...) FILTER (WHERE ...)``.
    """
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
        .where(
            attr.c.attribute_name.in_(
                [mapping.attribute_name for mapping in mappings]
            )
        )
        .cte("latest")
    )
    latest_current = select(latest).where(latest.c.rn == 1).subquery("cur")

    pivot_cols = [
        _pivot_aggregate(latest_current, mapping) for mapping in mappings
    ]

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
        .group_by(
            entity_table.c.id,
            entity_table.c.created_at,
        )
    )


def _construct_view(  # noqa: PLR0913
    tablename: str,
    schemaname: str,
    metadata: MetaData,
    mappings: list[_EAVMapping],
    entity_table: Table,
    attribute_table: Table,
) -> Table:
    """Register the pivot view and return a Table proxy."""
    view_sql = compile_query(
        _build_pivot_query(entity_table, attribute_table, mappings)
    )

    register_view(
        metadata,
        View(tablename, view_sql, schema=schemaname),
    )

    return Table(
        tablename,
        MetaData(),
        Column("id", Integer, primary_key=True),
        Column("created_at", DateTime(timezone=True)),
        *[
            Column(mapping.attribute_name, mapping.column_type)
            for mapping in mappings
        ],
        schema=schemaname,
    )


def _construct_api_view(
    tablename: str,
    mappings: list[_EAVMapping],
    api_schema: str,
    view_table: Table,
) -> View:
    """Build the API view that selects from the pivot view."""
    view_query = select(
        view_table.c["id"].label("id"),
        view_table.c["created_at"].label("created_at"),
        *[
            getattr(view_table.c, mapping.attribute_name).label(
                mapping.attribute_name
            )
            for mapping in mappings
        ],
    ).select_from(view_table)

    return View(
        tablename,
        compile_query(view_query),
        schema=api_schema,
    )


class EAVDimensionFactory(DimensionFactory):
    """Create an EAV dimension with entity and attribute tables.

    Produces ``<tablename>_entity`` (entity root),
    ``<tablename>_attribute`` (typed attribute rows with a CHECK
    constraint ensuring exactly one value column is populated),
    and a ``<tablename>`` pivot view that reconstructs the
    columnar form.  Also registers an API view and INSTEAD OF
    triggers.
    """

    def create_tables(self, ctx: FactoryContext) -> None:
        """Create the entity and attribute tables."""
        mappings = _build_eav_mappings(ctx.dimensions)
        ctx.kwargs["_eav_mappings"] = mappings

        ctx.tables["entity"] = _construct_entity_table(
            tablename=ctx.tablename,
            schemaname=ctx.schemaname,
            metadata=ctx.metadata,
            config_id=ctx.config.id_field_name,
        )

        ctx.tables["attribute"] = _construct_attribute_table(
            tablename=ctx.tablename,
            schemaname=ctx.schemaname,
            metadata=ctx.metadata,
            config_id=ctx.config.id_field_name,
            entity_table=ctx.tables["entity"],
            mappings=mappings,
        )

    def create_views(self, ctx: FactoryContext) -> None:
        """Create the pivot view and API view."""
        mappings: list[_EAVMapping] = ctx.kwargs["_eav_mappings"]

        view_table = _construct_view(
            tablename=ctx.tablename,
            schemaname=ctx.schemaname,
            metadata=ctx.metadata,
            mappings=mappings,
            entity_table=ctx.tables["entity"],
            attribute_table=ctx.tables["attribute"],
        )
        ctx.tables["view"] = view_table

        api_view = _construct_api_view(
            tablename=ctx.tablename,
            mappings=mappings,
            api_schema=ctx.api_configuration.schema_name,
            view_table=view_table,
        )
        register_view(ctx.metadata, api_view)
        ctx.views["api"] = api_view

    def create_triggers(self, ctx: FactoryContext) -> None:
        """Register INSTEAD OF triggers on both views."""
        mappings: list[_EAVMapping] = ctx.kwargs["_eav_mappings"]
        entity_table = ctx.tables["entity"]
        attribute_table = ctx.tables["attribute"]
        entity_fullname = f"{ctx.schemaname}.{entity_table.name}"
        attr_fullname = f"{ctx.schemaname}.{attribute_table.name}"

        mapping_tuples = [
            (mapping.attribute_name, mapping.value_column)
            for mapping in mappings
        ]
        template_vars = {
            "entity_table": entity_fullname,
            "attr_table": attr_fullname,
            "mappings": mapping_tuples,
        }

        ops = [
            ("insert", load_template("eav_insert.mako")),
            ("update", load_template("eav_update.mako")),
            ("delete", load_template("eav_delete.mako")),
        ]

        for view_schema, view_fullname in [
            (
                ctx.schemaname,
                f"{ctx.schemaname}.{ctx.tablename}",
            ),
            (
                ctx.api_configuration.schema_name,
                f"{ctx.api_configuration.schema_name}.{ctx.tablename}",
            ),
        ]:
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
