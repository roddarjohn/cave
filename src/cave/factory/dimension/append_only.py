from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Integer,
    MetaData,
    Table,
    select,
)
from sqlalchemy.schema import SchemaItem
from sqlalchemy_declarative_extensions import (
    View,
    register_view,
)

from cave.factory.dimension.types import DimensionConfiguration
from cave.factory.dimension.validator import validate_schema_items
from cave.resource import APIResource, register_api_resource
from cave.utils.naming import resolve_name
from cave.utils.query import compile_query
from cave.utils.template import load_template
from cave.utils.trigger import register_view_triggers

# Default naming convention templates for append-only tables.
_NAMING_DEFAULTS = {
    "append_only_root": "%(table_name)s_root",
    "append_only_attributes": "%(table_name)s_attributes",
    "append_only_function": "%(schema)s_%(table_name)s_%(op)s",
    "append_only_trigger": "%(schema)s_%(table_name)s_%(op)s",
}


def _construct_attribute_table(
    tablename: str,
    schemaname: str,
    metadata: MetaData,
    dimensions: list[SchemaItem],
    config: DimensionConfiguration,
) -> Table:
    attributes_tablename = resolve_name(
        metadata,
        "append_only_attributes",
        {"table_name": tablename, "schema": schemaname},
        _NAMING_DEFAULTS,
    )

    attributes_columns = [
        Column(config.id_field_name, Integer, primary_key=True),
        Column("created_at", DateTime(timezone=True), server_default="now()"),
        *dimensions,
    ]

    return Table(
        attributes_tablename,
        metadata,
        *attributes_columns,
        schema=schemaname,
    )


def _construct_root_table(
    tablename: str,
    schemaname: str,
    metadata: MetaData,
    config: DimensionConfiguration,
    attributes_table: Table,
) -> Table:
    root_tablename = resolve_name(
        metadata,
        "append_only_root",
        {"table_name": tablename, "schema": schemaname},
        _NAMING_DEFAULTS,
    )

    root_columns = [
        Column(config.id_field_name, Integer, primary_key=True),
        Column("created_at", DateTime(timezone=True), server_default="now()"),
        Column(
            f"{attributes_table.name}_id",
            ForeignKey(f"{schemaname}.{attributes_table.name}.id"),
        ),
    ]

    return Table(
        root_tablename,
        metadata,
        *root_columns,
        schema=schemaname,
    )


def _construct_view(  # noqa: PLR0913
    tablename: str,
    schemaname: str,
    metadata: MetaData,
    dimensions: list[SchemaItem],
    config: DimensionConfiguration,
    root_table: Table,
    attribute_table: Table,
) -> Table:
    view_query = (
        select(
            root_table.c["id"].label("id"),
            root_table.c["created_at"].label("created_at"),
            attribute_table.c["created_at"].label("updated_at"),
            *[
                dimension_column.label(dimension_column.key)
                for dimension_column in dimensions
                if isinstance(dimension_column, Column)
            ],
        )
        .select_from(root_table)
        .join(
            attribute_table,
            attribute_table.c[config.id_field_name]
            == root_table.c[f"{attribute_table.name}_id"],
        )
    )

    register_view(
        metadata,
        View(
            tablename,
            compile_query(view_query),
            schema=schemaname,
        ),
    )

    return Table(
        tablename,
        MetaData(),
        Column("id", Integer, primary_key=True),
        Column("created_at", DateTime(timezone=True)),
        Column("updated_at", DateTime(timezone=True)),
        *[
            Column(
                dimension_column.key,
                dimension_column.type,
            )
            for dimension_column in dimensions
            if isinstance(dimension_column, Column)
        ],
        schema=schemaname,
    )


def _construct_api_view(
    tablename: str,
    dimensions: list[SchemaItem],
    config: DimensionConfiguration,
    view_table: Table,
) -> View:
    view_query = select(
        view_table.c["id"].label("id"),
        view_table.c["created_at"].label("created_at"),
        view_table.c["updated_at"].label("updated_at"),
        *[
            getattr(view_table.c, dimension_column.key).label(
                dimension_column.key
            )
            for dimension_column in dimensions
            if isinstance(dimension_column, Column)
        ],
    ).select_from(view_table)

    return View(
        tablename,
        compile_query(view_query),
        schema=config.api_schema_name,
    )


def _dim_columns(dimensions: list[SchemaItem]) -> list[str]:
    """Extract column names from the dimension list."""
    return [c.key for c in dimensions if isinstance(c, Column)]


def _register_triggers(  # noqa: PLR0913
    tablename: str,
    schemaname: str,
    metadata: MetaData,
    dimensions: list[SchemaItem],
    config: DimensionConfiguration,
    root_table: Table,
    attribute_table: Table,
) -> None:
    """Register INSTEAD OF triggers on both the private and API views."""
    root_fullname = f"{schemaname}.{root_table.name}"
    attr_fullname = f"{schemaname}.{attribute_table.name}"

    dim_cols = _dim_columns(dimensions)
    template_vars = {
        "attr_table": attr_fullname,
        "root_table": root_fullname,
        "attr_cols": ", ".join(dim_cols),
        "new_cols": ", ".join(f"NEW.{c}" for c in dim_cols),
        "attr_fk_col": f"{attribute_table.name}_id",
    }

    ops = [
        ("insert", load_template("append_only_insert.mako")),
        ("update", load_template("append_only_update.mako")),
        ("delete", load_template("append_only_delete.mako")),
    ]

    for view_schema, view_fullname in [
        (schemaname, f"{schemaname}.{tablename}"),
        (
            config.api_schema_name,
            f"{config.api_schema_name}.{tablename}",
        ),
    ]:
        register_view_triggers(
            metadata=metadata,
            view_schema=view_schema,
            view_fullname=view_fullname,
            tablename=tablename,
            template_vars=template_vars,
            ops=ops,
            naming_defaults=_NAMING_DEFAULTS,
            function_key="append_only_function",
            trigger_key="append_only_trigger",
        )


def append_only_log_dimension_factory(
    tablename: str,
    schemaname: str,
    metadata: MetaData,
    dimensions: list[SchemaItem],
    config: DimensionConfiguration | None = None,
) -> None:
    """Create an append-only log dimension table and its root table.

    Creates three objects: ``<tablename>_attributes`` (the append-only log),
    ``<tablename>_root`` (the entity root with a FK to the latest attributes
    row), and a ``<tablename>`` view joining them.

    Also registers INSTEAD OF trigger functions on both the private and API
    views to support INSERT, UPDATE, and DELETE.

    :param tablename: Base name for the generated tables and view.
    :param schemaname: PostgreSQL schema for all generated objects.
    :param metadata: SQLAlchemy ``MetaData`` the tables are bound to.
    :param dimensions: Column definitions for the attribute columns.  Must
        not include a primary key column.
    :param config: Factory configuration; defaults to
        ``DimensionConfiguration()``.
    :raises CaveValidationError: If any item in *dimensions* fails validation.
    """
    config = config or DimensionConfiguration()

    validate_schema_items(dimensions)

    attributes_table = _construct_attribute_table(
        tablename=tablename,
        schemaname=schemaname,
        metadata=metadata,
        dimensions=dimensions,
        config=config,
    )

    root_table = _construct_root_table(
        tablename=tablename,
        schemaname=schemaname,
        metadata=metadata,
        config=config,
        attributes_table=attributes_table,
    )

    view_table = _construct_view(
        tablename=tablename,
        schemaname=schemaname,
        metadata=metadata,
        dimensions=dimensions,
        config=config,
        root_table=root_table,
        attribute_table=attributes_table,
    )

    api_view = _construct_api_view(
        tablename=tablename,
        dimensions=dimensions,
        config=config,
        view_table=view_table,
    )

    register_view(metadata, api_view)
    register_api_resource(
        metadata,
        APIResource(
            name=tablename,
            schema=config.api_schema_name,
            grants=["select", "insert", "update", "delete"],
        ),
    )

    _register_triggers(
        tablename=tablename,
        schemaname=schemaname,
        metadata=metadata,
        dimensions=dimensions,
        config=config,
        root_table=root_table,
        attribute_table=attributes_table,
    )
