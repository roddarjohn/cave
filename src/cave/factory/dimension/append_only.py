from typing import Any

from alembic_utils.pg_view import PGView
from alembic_utils.replaceable_entity import registry
from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Integer,
    MetaData,
    Table,
    select,
)
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import SchemaItem

from cave.factory.dimension.types import DimensionConfiguration
from cave.factory.dimension.validator import validate_schema_items


def append_only_log_dimension_factory(
    tablename: str,
    schemaname: str,
    metadata: MetaData,
    dimensions: list[SchemaItem],
    config: DimensionConfiguration | None = None,
    **kwargs: Any,  # noqa: ANN401
) -> None:
    """Create an append-only log dimension table and its root table."""
    config = config or DimensionConfiguration()

    validate_schema_items(dimensions)

    attributes_tablename = f"{tablename}_attributes"
    attributes_columns = [
        Column(config.id_field_name, Integer, primary_key=True),
        Column("created_at", DateTime(timezone=True), server_default="now()"),
        *dimensions,
    ]

    attribute_table = Table(
        attributes_tablename,
        metadata,
        *attributes_columns,
        schema=schemaname,
        **kwargs,
    )

    root_tablename = f"{tablename}_root"
    root_columns = [
        Column(config.id_field_name, Integer, primary_key=True),
        Column("created_at", DateTime(timezone=True), server_default="now()"),
        Column(
            f"{attributes_tablename}_id",
            ForeignKey(f"{schemaname}.{attributes_tablename}.id"),
        ),
    ]

    root_table = Table(
        root_tablename,
        metadata,
        *root_columns,
        schema=schemaname,
        **kwargs,
    )

    # View definition

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
            attribute_table.c[config.id_field_name] == root_table.c["id"],
        )
    )

    registry.register(
        [
            PGView(
                schema=schemaname,
                signature=tablename,
                definition=str(
                    view_query.compile(
                        dialect=postgresql.dialect(),
                        compile_kwargs={"literal_binds": True},
                    )
                ),
            )
        ]
    )
