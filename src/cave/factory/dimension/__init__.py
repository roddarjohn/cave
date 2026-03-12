"""Dimension factories for Cave.

Provides plugin-based dimension table factories that generate
SQLAlchemy models, views, and PostgREST API resources from
declarative configurations.
"""

from cave.factory.dimension.append_only import (
    AppendOnlyDimensionFactory,
)
from cave.factory.dimension.base import (
    DimensionFactory,
    FactoryContext,
)
from cave.factory.dimension.eav import EAVDimensionFactory
from cave.factory.dimension.simple import SimpleDimensionFactory
from cave.factory.dimension.types import (
    APIResourceConfiguration,
    DimensionConfiguration,
)

__all__ = [
    "APIResourceConfiguration",
    "AppendOnlyDimensionFactory",
    "DimensionConfiguration",
    "DimensionFactory",
    "EAVDimensionFactory",
    "FactoryContext",
    "SimpleDimensionFactory",
]
