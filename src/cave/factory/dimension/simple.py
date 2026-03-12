"""Simple dimension factory.

Creates a single table with an auto-generated primary key,
an API view, and INSTEAD OF triggers for full CRUD.
"""

from sqlalchemy import Column, Integer, Table, select
from sqlalchemy.schema import SchemaItem
from sqlalchemy_declarative_extensions import (
    View,
    register_view,
)

from cave.factory.dimension.base import DimensionFactory, FactoryContext
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
        col.key
        for col in dimensions
        if isinstance(col, Column) and not col.primary_key
    ]


class SimpleDimensionFactory(DimensionFactory):
    """Create a simple dimension table with an auto-generated PK.

    Produces a single table, an API view selecting from it, and
    INSTEAD OF triggers for insert/update/delete on the API view.
    """

    def create_tables(self, ctx: FactoryContext) -> None:
        """Create the dimension table with an auto-increment PK."""
        ctx.dimensions.insert(
            0,
            Column(ctx.config.id_field_name, Integer, primary_key=True),
        )

        ctx.tables["base"] = Table(
            ctx.tablename,
            ctx.metadata,
            *ctx.dimensions,
            schema=ctx.schemaname,
            **ctx.kwargs,
        )

    def create_views(self, ctx: FactoryContext) -> None:
        """Create the API view selecting from the base table."""
        table = ctx.tables["base"]
        api_query = select(
            *[col.label(col.key) for col in table.columns]
        ).select_from(table)
        api_view = View(
            ctx.tablename,
            compile_query(api_query),
            schema=ctx.api_configuration.schema_name,
        )
        register_view(ctx.metadata, api_view)
        ctx.views["api"] = api_view

    def create_triggers(self, ctx: FactoryContext) -> None:
        """Register INSTEAD OF triggers on the API view."""
        base_fullname = f"{ctx.schemaname}.{ctx.tablename}"
        dim_cols = _dim_columns(ctx.dimensions)

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
            metadata=ctx.metadata,
            view_schema=ctx.api_configuration.schema_name,
            view_fullname=(
                f"{ctx.api_configuration.schema_name}.{ctx.tablename}"
            ),
            tablename=ctx.tablename,
            template_vars=template_vars,
            ops=ops,
            naming_defaults=_NAMING_DEFAULTS,
            function_key="simple_function",
            trigger_key="simple_trigger",
        )
