"""EAV (Entity-Attribute-Value) dimension factory.

Creates an entity table and an append-only typed attribute table
with a CHECK constraint enforcing exactly one value column per
row.  Multiple rows per ``(entity_id, attribute_name)`` are
allowed, providing full audit history.  A pivot view reconstructs
the current columnar representation using the latest attribute
row per entity.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import cast

from mako.template import Template
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
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.schema import SchemaItem
from sqlalchemy_declarative_extensions import (
    View,
    register_function,
    register_trigger,
    register_view,
)
from sqlalchemy_declarative_extensions.dialects.postgresql import (
    Function,
    FunctionSecurity,
    Trigger,
)

from cave.factory.dimension.types import DimensionConfiguration
from cave.factory.dimension.utils import CaveValidationError
from cave.factory.dimension.validator import validate_schema_items
from cave.resource import APIResource, register_api_resource
from cave.utils.query import compile_query

_NAMING_DEFAULTS = {
    "eav_entity": "%(table_name)s_entity",
    "eav_attribute": "%(table_name)s_attribute",
    "eav_function": "%(schema)s_%(table_name)s_%(op)s",
    "eav_trigger": "%(schema)s_%(table_name)s_%(op)s",
}

_TEMPLATE_DIR = Path(__file__).parent / "templates"

# Maps SQLAlchemy type classes to (value_column_name, column_type).
_TYPE_MAP: dict[type, tuple[str, sa_types.TypeEngine]] = {
    sa_types.Integer: ("integer_value", sa_types.Integer()),
    sa_types.SmallInteger: ("integer_value", sa_types.Integer()),
    sa_types.BigInteger: (
        "integer_value",
        sa_types.BigInteger(),
    ),
    sa_types.String: ("text_value", sa_types.Text()),
    sa_types.Text: ("text_value", sa_types.Text()),
    sa_types.Boolean: (
        "boolean_value",
        sa_types.Boolean(),
    ),
    sa_types.Float: ("float_value", sa_types.Float()),
    sa_types.Double: ("float_value", sa_types.Float()),
    sa_types.Numeric: (
        "float_value",
        sa_types.Numeric(),
    ),
    sa_types.Date: ("date_value", sa_types.Date()),
    sa_types.DateTime: (
        "timestamp_value",
        sa_types.DateTime(timezone=True),
    ),
    JSONB: ("jsonb_value", JSONB()),
}


@dataclass
class _EAVMapping:
    """Internal mapping from a dimension name to EAV storage."""

    attribute_name: str
    value_column: str
    column_type: sa_types.TypeEngine


def _load_template(name: str) -> Template:
    """Load a Mako template from the templates directory."""
    return Template(  # noqa: S702
        filename=str(_TEMPLATE_DIR / name)
    )


def _resolve_name(
    metadata: MetaData,
    key: str,
    substitutions: dict[str, str],
) -> str:
    """Resolve a name using the naming convention or default."""
    template = cast(
        "str",
        metadata.naming_convention.get(key, _NAMING_DEFAULTS[key]),
    )
    return template % substitutions


def _resolve_value_column(
    col: Column,
) -> tuple[str, sa_types.TypeEngine]:
    """Map a Column's type to its EAV value column.

    Walks the MRO of the column's type class so that
    subclasses (e.g. ``SmallInteger``) resolve without
    explicit entries for every variant.

    :raises CaveValidationError: If no mapping exists.
    """
    col_type = type(col.type)
    for cls in col_type.__mro__:
        if cls in _TYPE_MAP:
            return _TYPE_MAP[cls]
    msg = f"No EAV value column mapping for type {col_type.__name__}"
    raise CaveValidationError(msg)


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
    for m in mappings:
        if m.value_column not in cols:
            cols[m.value_column] = m.column_type
    return cols


def _construct_entity_table(
    tablename: str,
    schemaname: str,
    metadata: MetaData,
    config: DimensionConfiguration,
) -> Table:
    """Create the ``{tablename}_entity`` table."""
    entity_tablename = _resolve_name(
        metadata,
        "eav_entity",
        {"table_name": tablename, "schema": schemaname},
    )
    return Table(
        entity_tablename,
        metadata,
        Column(config.id_field_name, Integer, primary_key=True),
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
    config: DimensionConfiguration,
    entity_table: Table,
    mappings: list[_EAVMapping],
) -> Table:
    """Create the ``{tablename}_attribute`` table.

    Includes only the value columns actually used and a CHECK
    constraint enforcing exactly one non-null value column.
    No unique constraint -- multiple rows per
    ``(entity_id, attribute_name)`` provide full audit history.
    """
    attr_tablename = _resolve_name(
        metadata,
        "eav_attribute",
        {"table_name": tablename, "schema": schemaname},
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

    entity_fq = f"{schemaname}.{entity_table.name}.{config.id_field_name}"

    return Table(
        attr_tablename,
        metadata,
        Column(config.id_field_name, Integer, primary_key=True),
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
    m: _EAVMapping,
) -> Label:
    """Build a MAX(...) FILTER (...) pivot expression.

    PostgreSQL lacks ``MAX(boolean)``, so boolean columns are
    cast to integer for aggregation, then back to boolean.
    """
    col = subquery.c[m.value_column]
    condition = subquery.c.attribute_name == literal(m.attribute_name)
    if isinstance(m.column_type, sa_types.Boolean):
        return sa_cast(
            func.max(sa_cast(col, Integer)).filter(condition),
            sa_types.Boolean(),
        ).label(m.attribute_name)
    return func.max(col).filter(condition).label(m.attribute_name)


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
    a = attribute_table
    rn = (
        func.row_number()
        .over(
            partition_by=[a.c.entity_id, a.c.attribute_name],
            order_by=[a.c.created_at.desc(), a.c.id.desc()],
        )
        .label("rn")
    )
    latest = (
        select(a, rn)
        .where(a.c.attribute_name.in_([m.attribute_name for m in mappings]))
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
        *[Column(m.attribute_name, m.column_type) for m in mappings],
        schema=schemaname,
    )


def _construct_api_view(
    tablename: str,
    mappings: list[_EAVMapping],
    config: DimensionConfiguration,
    view_table: Table,
) -> View:
    """Build the API view that selects from the pivot view."""
    view_query = select(
        view_table.c["id"].label("id"),
        view_table.c["created_at"].label("created_at"),
        *[
            getattr(view_table.c, m.attribute_name).label(m.attribute_name)
            for m in mappings
        ],
    ).select_from(view_table)

    return View(
        tablename,
        compile_query(view_query),
        schema=config.api_schema_name,
    )


def _register_triggers(  # noqa: PLR0913
    tablename: str,
    schemaname: str,
    metadata: MetaData,
    mappings: list[_EAVMapping],
    config: DimensionConfiguration,
    entity_table: Table,
    attribute_table: Table,
) -> None:
    """Register INSTEAD OF triggers on the pivot and API views."""
    entity_fullname = f"{schemaname}.{entity_table.name}"
    attr_fullname = f"{schemaname}.{attribute_table.name}"

    mapping_tuples = [(m.attribute_name, m.value_column) for m in mappings]
    template_vars = {
        "entity_table": entity_fullname,
        "attr_table": attr_fullname,
        "mappings": mapping_tuples,
    }

    views_to_trigger = [
        (schemaname, f"{schemaname}.{tablename}"),
        (
            config.api_schema_name,
            f"{config.api_schema_name}.{tablename}",
        ),
    ]

    ops = [
        ("insert", _load_template("eav_insert.mako")),
        ("update", _load_template("eav_update.mako")),
        ("delete", _load_template("eav_delete.mako")),
    ]

    for view_schema, view_fullname in views_to_trigger:
        subs = {
            "table_name": tablename,
            "schema": view_schema,
        }

        for op, template in ops:
            fn_name = _resolve_name(
                metadata,
                "eav_function",
                {**subs, "op": op},
            )
            trigger_name = _resolve_name(
                metadata,
                "eav_trigger",
                {**subs, "op": op},
            )

            register_function(
                metadata,
                Function(
                    fn_name,
                    template.render(**template_vars),
                    returns="trigger",
                    language="plpgsql",
                    schema=view_schema,
                    security=FunctionSecurity.definer,
                ),
            )

            register_trigger(
                metadata,
                Trigger.instead_of(
                    op,
                    on=view_fullname,
                    execute=f"{view_schema}.{fn_name}",
                    name=trigger_name,
                ).for_each_row(),
            )


def eav_dimension_factory(
    tablename: str,
    schemaname: str,
    metadata: MetaData,
    dimensions: list[SchemaItem],
    config: DimensionConfiguration | None = None,
) -> None:
    """Create an EAV dimension with entity and attribute tables.

    Creates ``<tablename>_entity`` (entity root),
    ``<tablename>_attribute`` (typed attribute rows with a CHECK
    constraint ensuring exactly one value column is populated),
    and a ``<tablename>`` pivot view that reconstructs the
    columnar form.

    Also registers an API view, PostgREST grants, and INSTEAD OF
    triggers for INSERT, UPDATE, and DELETE.

    :param tablename: Base name for the generated objects.
    :param schemaname: PostgreSQL schema for all generated objects.
    :param metadata: SQLAlchemy ``MetaData`` bound to.
    :param dimensions: Column definitions for the EAV attributes.
        Each column's SQLAlchemy type is mapped to a value column.
        Must not include a primary key column.
    :param config: Factory configuration; defaults to
        ``DimensionConfiguration()``.
    :raises CaveValidationError: If any dimension fails validation
        or has an unmappable type.
    """
    config = config or DimensionConfiguration()

    validate_schema_items(dimensions)

    mappings = _build_eav_mappings(dimensions)

    entity_table = _construct_entity_table(
        tablename=tablename,
        schemaname=schemaname,
        metadata=metadata,
        config=config,
    )

    attribute_table = _construct_attribute_table(
        tablename=tablename,
        schemaname=schemaname,
        metadata=metadata,
        config=config,
        entity_table=entity_table,
        mappings=mappings,
    )

    view_table = _construct_view(
        tablename=tablename,
        schemaname=schemaname,
        metadata=metadata,
        mappings=mappings,
        entity_table=entity_table,
        attribute_table=attribute_table,
    )

    api_view = _construct_api_view(
        tablename=tablename,
        mappings=mappings,
        config=config,
        view_table=view_table,
    )

    register_view(metadata, api_view)
    register_api_resource(
        metadata,
        APIResource(
            name=tablename,
            schema=config.api_schema_name,
            grants=[
                "select",
                "insert",
                "update",
                "delete",
            ],
        ),
    )

    _register_triggers(
        tablename=tablename,
        schemaname=schemaname,
        metadata=metadata,
        mappings=mappings,
        config=config,
        entity_table=entity_table,
        attribute_table=attribute_table,
    )
