from typing import Any

from sqlalchemy import Column, Integer, MetaData, Table, select
from sqlalchemy.schema import SchemaItem
from sqlalchemy_declarative_extensions import (
    View,
    register_view,
)

from cave.factory.dimension.types import DimensionConfiguration
from cave.factory.dimension.validator import validate_schema_items
from cave.resource import APIResource, register_api_resource
from cave.utils.query import compile_query
from cave.utils.template import load_template
from cave.utils.trigger import register_view_triggers

_NAMING_DEFAULTS = {
    "simple_function": "%(schema)s_%(table_name)s_%(op)s",
    "simple_trigger": "%(schema)s_%(table_name)s_%(op)s",
}


def _dim_columns(dimensions: list[SchemaItem]) -> list[str]:
    """Extract non-PK column names from the dimension list."""
    return [
        c.key for c in dimensions if isinstance(c, Column) and not c.primary_key
    ]


def _register_triggers(
    tablename: str,
    schemaname: str,
    metadata: MetaData,
    dimensions: list[SchemaItem],
    config: DimensionConfiguration,
) -> None:
    """Register INSTEAD OF triggers on the API view."""
    base_fullname = f"{schemaname}.{tablename}"
    dim_cols = _dim_columns(dimensions)

    template_vars = {
        "base_table": base_fullname,
        "cols": ", ".join(dim_cols),
        "new_cols": ", ".join(f"NEW.{c}" for c in dim_cols),
        "set_clause": ", ".join(f"{c} = NEW.{c}" for c in dim_cols),
    }

    ops = [
        ("insert", load_template("simple_insert.mako")),
        ("update", load_template("simple_update.mako")),
        ("delete", load_template("simple_delete.mako")),
    ]

    register_view_triggers(
        metadata=metadata,
        view_schema=config.api_schema_name,
        view_fullname=f"{config.api_schema_name}.{tablename}",
        tablename=tablename,
        template_vars=template_vars,
        ops=ops,
        naming_defaults=_NAMING_DEFAULTS,
        function_key="simple_function",
        trigger_key="simple_trigger",
    )


def simple_dimension_factory(  # noqa: PLR0913
    tablename: str,
    schemaname: str,
    metadata: MetaData,
    dimensions: list[SchemaItem],
    config: DimensionConfiguration | None = None,
    grants: list[str] | None = None,
    **kwargs: Any,  # noqa: ANN401
) -> Table:
    """Create a simple dimension table with an auto-generated primary key.

    :param tablename: Name of the dimension table.
    :param schemaname: PostgreSQL schema to create the table in.
    :param metadata: SQLAlchemy ``MetaData`` the table is bound to.
    :param dimensions: Column and constraint definitions for the dimension
        attributes.  Must not include a primary key column.
    :param config: Factory configuration; defaults to
        ``DimensionConfiguration()``.
    :param grants: Privileges to grant on the API view.  Defaults to
        ``["select"]``.
    :param kwargs: Extra keyword arguments forwarded to ``Table()``.
    :returns: The created ``Table`` object.
    :raises CaveValidationError: If any item in *dimensions* fails validation.
    """
    config = config or DimensionConfiguration()

    validate_schema_items(dimensions)

    dimensions.insert(
        0,
        Column(config.id_field_name, Integer, primary_key=True),
    )

    table = Table(
        tablename,
        metadata,
        *dimensions,
        schema=schemaname,
        **kwargs,
    )

    api_query = select(*[c.label(c.key) for c in table.columns]).select_from(
        table
    )
    register_view(
        metadata,
        View(
            tablename,
            compile_query(api_query),
            schema=config.api_schema_name,
        ),
    )
    register_api_resource(
        metadata,
        APIResource(
            name=tablename,
            schema=config.api_schema_name,
            grants=grants or ["select"],
        ),
    )

    _register_triggers(
        tablename=tablename,
        schemaname=schemaname,
        metadata=metadata,
        dimensions=dimensions,
        config=config,
    )

    return table
