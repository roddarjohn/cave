"""Simple dimension table factory."""

from sqlalchemy import Column, Integer, MetaData, Table
from sqlalchemy.schema import SchemaItem

from cave.factory.dimension.types import DimensionConfiguration
from cave.factory.dimension.validator import validate_schema_items


def simple_dimension_factory(
    tablename: str,
    schemaname: str,
    metadata: MetaData,
    dimensions: list[SchemaItem],
    config: DimensionConfiguration | None = None,
) -> Table:
    """Create a simple dimension table with an auto-generated primary key."""
    config = config or DimensionConfiguration()

    validate_schema_items(dimensions)

    dimensions.insert(
        0,
        Column(config.id_field_name, Integer, primary_key=True),
    )

    return Table(
        tablename,
        metadata,
        *dimensions,
        schema=schemaname,
    )
