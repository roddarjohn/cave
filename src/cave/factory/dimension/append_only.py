"""Append-only log dimension factory.

Creates a root table, an append-only attributes table, a join
view, an API view, and INSTEAD OF triggers for full CRUD.
"""

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

from cave.factory.dimension.base import DimensionFactory, FactoryContext
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
    config_id: str,
) -> Table:
    """Create the attributes table."""
    attributes_tablename = resolve_name(
        metadata,
        "append_only_attributes",
        {"table_name": tablename, "schema": schemaname},
        _NAMING_DEFAULTS,
    )

    attributes_columns = [
        Column(config_id, Integer, primary_key=True),
        Column(
            "created_at",
            DateTime(timezone=True),
            server_default="now()",
        ),
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
    config_id: str,
    attributes_table: Table,
) -> Table:
    """Create the root table."""
    root_tablename = resolve_name(
        metadata,
        "append_only_root",
        {"table_name": tablename, "schema": schemaname},
        _NAMING_DEFAULTS,
    )

    root_columns = [
        Column(config_id, Integer, primary_key=True),
        Column(
            "created_at",
            DateTime(timezone=True),
            server_default="now()",
        ),
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
    config_id: str,
    root_table: Table,
    attribute_table: Table,
) -> Table:
    """Register the join view and return a Table proxy."""
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
            attribute_table.c[config_id]
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
    api_schema: str,
    view_table: Table,
) -> View:
    """Build the API view selecting from the join view."""
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
        schema=api_schema,
    )


def _dim_columns(dimensions: list[SchemaItem]) -> list[str]:
    """Extract column names from the dimension list."""
    return [col.key for col in dimensions if isinstance(col, Column)]


class AppendOnlyDimensionFactory(DimensionFactory):
    """Create an append-only log dimension.

    Produces ``<tablename>_attributes`` (the append-only log),
    ``<tablename>_root`` (the entity root with a FK to the
    latest attributes row), and a ``<tablename>`` join view.
    Also registers an API view and INSTEAD OF triggers.
    """

    def create_tables(self, ctx: FactoryContext) -> None:
        """Create the root and attributes tables."""
        ctx.tables["attributes"] = _construct_attribute_table(
            tablename=ctx.tablename,
            schemaname=ctx.schemaname,
            metadata=ctx.metadata,
            dimensions=ctx.dimensions,
            config_id=ctx.config.id_field_name,
        )

        ctx.tables["root"] = _construct_root_table(
            tablename=ctx.tablename,
            schemaname=ctx.schemaname,
            metadata=ctx.metadata,
            config_id=ctx.config.id_field_name,
            attributes_table=ctx.tables["attributes"],
        )

    def create_views(self, ctx: FactoryContext) -> None:
        """Create the private join view and API view."""
        view_table = _construct_view(
            tablename=ctx.tablename,
            schemaname=ctx.schemaname,
            metadata=ctx.metadata,
            dimensions=ctx.dimensions,
            config_id=ctx.config.id_field_name,
            root_table=ctx.tables["root"],
            attribute_table=ctx.tables["attributes"],
        )
        ctx.tables["view"] = view_table

        api_view = _construct_api_view(
            tablename=ctx.tablename,
            dimensions=ctx.dimensions,
            api_schema=ctx.api_configuration.schema_name,
            view_table=view_table,
        )
        register_view(ctx.metadata, api_view)
        ctx.views["api"] = api_view

    def create_triggers(self, ctx: FactoryContext) -> None:
        """Register INSTEAD OF triggers on both views."""
        root_table = ctx.tables["root"]
        attribute_table = ctx.tables["attributes"]
        root_fullname = f"{ctx.schemaname}.{root_table.name}"
        attr_fullname = f"{ctx.schemaname}.{attribute_table.name}"

        dim_cols = _dim_columns(ctx.dimensions)
        template_vars = {
            "attr_table": attr_fullname,
            "root_table": root_fullname,
            "attr_cols": ", ".join(dim_cols),
            "new_cols": ", ".join(f"NEW.{c}" for c in dim_cols),
            "attr_fk_col": f"{attribute_table.name}_id",
        }

        ops = [
            (
                "insert",
                load_template("append_only_insert.mako"),
            ),
            (
                "update",
                load_template("append_only_update.mako"),
            ),
            (
                "delete",
                load_template("append_only_delete.mako"),
            ),
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
                function_key="append_only_function",
                trigger_key="append_only_trigger",
            )
