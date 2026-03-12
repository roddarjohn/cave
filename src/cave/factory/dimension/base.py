"""Base class for dimension factories.

Provides a common orchestration flow — validate, create tables,
create views, register API resource, register triggers — with
hook methods that concrete subclasses implement.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import MetaData, Table
from sqlalchemy.schema import SchemaItem
from sqlalchemy_declarative_extensions import View

from cave.factory.dimension.types import (
    APIResourceConfiguration,
    DimensionConfiguration,
)
from cave.factory.dimension.validator import validate_schema_items
from cave.resource import APIResource, register_api_resource


@dataclass
class FactoryContext:
    """Carries inputs and accumulates outputs across factory hooks.

    Hooks store their created tables via ``ctx.tables["root"]``
    etc.  The generic ``_register_api`` reads ``ctx.tablename``,
    ``ctx.api_configuration``, and ``ctx.config``.
    """

    tablename: str
    schemaname: str
    metadata: MetaData
    dimensions: list[SchemaItem]
    config: DimensionConfiguration
    api_configuration: APIResourceConfiguration
    kwargs: dict[str, Any]

    tables: dict[str, Table] = field(default_factory=dict)
    views: dict[str, View] = field(default_factory=dict)


class DimensionFactory(ABC):
    """Base class for dimension factories.

    Subclasses implement the ``create_tables``, ``create_views``,
    and ``create_triggers`` hooks.  Instantiation runs the full
    orchestration flow::

        SimpleDimensionFactory(
            tablename="users",
            schemaname="public",
            metadata=metadata,
            dimensions=[Column("name", String)],
        )
    """

    def __init__(  # noqa: PLR0913
        self,
        tablename: str,
        schemaname: str,
        metadata: MetaData,
        dimensions: list[SchemaItem],
        config: DimensionConfiguration | None = None,
        api_configuration: APIResourceConfiguration | None = None,
        **kwargs: Any,  # noqa: ANN401
    ) -> None:
        """Create the dimension and register it on *metadata*.

        Args:
            tablename: Name of the dimension table.
            schemaname: PostgreSQL schema for all generated
                objects.
            metadata: SQLAlchemy ``MetaData`` the tables are
                bound to.
            dimensions: Column and constraint definitions.  Must
                not include a primary key column.
            config: Factory configuration; defaults to
                ``DimensionConfiguration()``.
            api_configuration: API resource configuration;
                defaults to ``APIResourceConfiguration()``.
            **kwargs: Extra keyword arguments forwarded to
                ``Table()``.

        Raises:
            CaveValidationError: If any item in *dimensions*
                fails validation.

        """
        config = config or DimensionConfiguration()
        api_configuration = api_configuration or APIResourceConfiguration()
        validate_schema_items(dimensions)

        ctx = FactoryContext(
            tablename=tablename,
            schemaname=schemaname,
            metadata=metadata,
            dimensions=dimensions,
            config=config,
            api_configuration=api_configuration,
            kwargs=kwargs,
        )

        self.create_tables(ctx)
        self.create_views(ctx)
        self._register_api(ctx)
        self.create_triggers(ctx)

    @abstractmethod
    def create_tables(self, ctx: FactoryContext) -> None:
        """Create the backing tables for this dimension type."""

    @abstractmethod
    def create_views(self, ctx: FactoryContext) -> None:
        """Create the views for this dimension type."""

    @abstractmethod
    def create_triggers(self, ctx: FactoryContext) -> None:
        """Register INSTEAD OF triggers on the views."""

    def _register_api(self, ctx: FactoryContext) -> None:
        """Register the PostgREST API resource."""
        register_api_resource(
            ctx.metadata,
            APIResource(
                name=ctx.tablename,
                schema=ctx.api_configuration.schema_name,
                grants=ctx.api_configuration.grants,
            ),
        )
