from pathlib import Path
from typing import Any, cast

from mako.template import Template
from sqlalchemy import Column, Integer, MetaData, Table, select
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

_NAMING_DEFAULTS = {
    "simple_function": "%(schema)s_%(table_name)s_%(op)s",
    "simple_trigger": "%(schema)s_%(table_name)s_%(op)s",
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
        ("insert", _load_template("simple_insert.mako")),
        ("update", _load_template("simple_update.mako")),
        ("delete", _load_template("simple_delete.mako")),
    ]

    subs = {"table_name": tablename, "schema": config.api_schema_name}
    view_fullname = f"{config.api_schema_name}.{tablename}"

    for op, template in ops:
        fn_name = _resolve_name(
            metadata,
            "simple_function",
            {**subs, "op": op},
        )
        trigger_name = _resolve_name(
            metadata,
            "simple_trigger",
            {**subs, "op": op},
        )

        register_function(
            metadata,
            Function(
                fn_name,
                template.render(**template_vars),
                returns="trigger",
                language="plpgsql",
                schema=config.api_schema_name,
                security=FunctionSecurity.definer,
            ),
        )

        register_trigger(
            metadata,
            Trigger.instead_of(
                op,
                on=view_fullname,
                execute=f"{config.api_schema_name}.{fn_name}",
                name=trigger_name,
            ).for_each_row(),
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
