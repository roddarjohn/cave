"""Generic view factories."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy_declarative_extensions import View, register_view

from pgcraft.utils.query import compile_query

if TYPE_CHECKING:
    from sqlalchemy import MetaData, Select


class PGCraftView:
    """Create a plain PostgreSQL view from a SQLAlchemy select.

    The resulting view object is available as ``self.view``
    and can be used in further queries or joins.

    Args:
        name: View name.
        schema: PostgreSQL schema for the view.
        metadata: SQLAlchemy ``MetaData`` to register on.
        query: A SQLAlchemy ``Select`` defining the view body.

    """

    def __init__(
        self,
        name: str,
        schema: str,
        metadata: MetaData,
        query: Select,
    ) -> None:
        """Create and register the view."""
        definition = compile_query(query)
        self.view = View(name, definition, schema=schema)
        register_view(metadata, self.view)
        self.name = name
        self.schema = schema
        self.metadata = metadata


class PGCraftMaterializedView:
    """Create a materialized view with an auto-generated refresh.

    Args:
        name: View name.
        schema: PostgreSQL schema for the view.
        metadata: SQLAlchemy ``MetaData`` to register on.
        query: A SQLAlchemy ``Select`` defining the view body.

    """

    def __init__(
        self,
        name: str,
        schema: str,
        metadata: MetaData,
        query: Select,
    ) -> None:
        """Create and register the materialized view."""
        definition = compile_query(query)
        self.view = View(
            name,
            definition,
            schema=schema,
            materialized=True,
        )
        register_view(metadata, self.view)
        self.name = name
        self.schema = schema
        self.metadata = metadata
