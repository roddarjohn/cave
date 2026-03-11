from pathlib import Path
from typing import cast

from mako.template import Template
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
from cave.factory.dimension.validator import validate_schema_items
from cave.resource import APIResource, register_api_resource
from cave.utils.query import compile_query

# Default naming convention templates for append-only tables.
_NAMING_DEFAULTS = {
    "append_only_root": "%(table_name)s_root",
    "append_only_attributes": "%(table_name)s_attributes",
    "append_only_function": "%(schema)s_%(table_name)s_%(op)s",
    "append_only_trigger": "%(schema)s_%(table_name)s_%(op)s",
}

_TEMPLATE_DIR = Path(__file__).parent / "templates"


def _load_template(name: str) -> Template:
    """Load a Mako template from the templates directory."""
    return Template(filename=str(_TEMPLATE_DIR / name))  # noqa: S702


def _resolve_name(
    metadata: MetaData,
    key: str,
    substitutions: dict[str, str],
) -> str:
    """Resolve a name using the metadata naming convention or a default."""
    template = cast(
        "str",
        metadata.naming_convention.get(key, _NAMING_DEFAULTS[key]),
    )
    return template % substitutions


def _construct_attribute_table(
    tablename: str,
    schemaname: str,
    metadata: MetaData,
    dimensions: list[SchemaItem],
    config: DimensionConfiguration,
) -> Table:
    attributes_tablename = _resolve_name(
        metadata,
        "append_only_attributes",
        {"table_name": tablename, "schema": schemaname},
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
    root_tablename = _resolve_name(
        metadata,
        "append_only_root",
        {"table_name": tablename, "schema": schemaname},
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

    views_to_trigger = [
        (schemaname, f"{schemaname}.{tablename}"),
        (config.api_schema_name, f"{config.api_schema_name}.{tablename}"),
    ]

    ops = [
        ("insert", _load_template("append_only_insert.mako")),
        ("update", _load_template("append_only_update.mako")),
        ("delete", _load_template("append_only_delete.mako")),
    ]

    for view_schema, view_fullname in views_to_trigger:
        subs = {"table_name": tablename, "schema": view_schema}

        for op, template in ops:
            fn_name = _resolve_name(
                metadata,
                "append_only_function",
                {**subs, "op": op},
            )
            trigger_name = _resolve_name(
                metadata,
                "append_only_trigger",
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
