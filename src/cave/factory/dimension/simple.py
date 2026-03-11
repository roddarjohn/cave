from typing import Any

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

    return Table(
        tablename,
        metadata,
        *dimensions,
        schema=schemaname,
        **kwargs,
    )
