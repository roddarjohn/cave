"""Append-only log dimension table factory."""

from sqlalchemy import Column, Integer, MetaData, Table
from sqlalchemy.schema import SchemaItem

from cave.factory.dimension.types import DimensionConfiguration
from cave.factory.dimension.validator import validate_schema_items


def append_only_log_dimension_factory(
    tablename: str,
    schemaname: str,
    metadata: MetaData,
    dimensions: list[SchemaItem],
    config: DimensionConfiguration | None = None,
) -> Table:
    """Create an append-only log dimension table and its root table."""
    config = config or DimensionConfiguration()

    validate_schema_items(dimensions)

    attributes_tablename = f"{tablename}_attributes"
    attributes_columns = [
        Column(config.id_field_name, Integer, primary_key=True),
        *dimensions,
    ]

    attributes_table = Table(
        attributes_tablename,
        metadata,
        *attributes_columns,
        schema=schemaname,
    )

    root_columns = [
        Column(config.id_field_name, Integer, primary_key=True),
    ]

    Table(
        tablename,
        metadata,
        *root_columns,
        schema=schemaname,
    )

    return attributes_table
