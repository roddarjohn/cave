"""Generic view factories."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import Column, MetaData, Table
from sqlalchemy_declarative_extensions import View, register_view

from pgcraft.utils.query import compile_query

if TYPE_CHECKING:
    from sqlalchemy import Select
    from sqlalchemy.sql.type_api import TypeEngine


def _column_type(col: object) -> TypeEngine:
    """Extract the SA type from a selected column."""
    from sqlalchemy import types as sa_types  # noqa: PLC0415

    return getattr(col, "type", sa_types.NullType())


def _table_from_query(
    name: str,
    schema: str,
    query: Select,
) -> Table:
    """Build a joinable ``Table`` proxy from a query.

    The table lives on a private ``MetaData`` so it does not
    interfere with Alembic autogeneration.
    """
    cols = [
        Column(c.key, _column_type(c))
        for c in query.selected_columns
        if c.key is not None
    ]
    return Table(name, MetaData(), *cols, schema=schema)


class PGCraftView:
    """Create a plain PostgreSQL view from a SQLAlchemy select.

    After construction, ``self.table`` is a joinable
    SQLAlchemy ``Table`` whose columns mirror the query.

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
        self.table = _table_from_query(name, schema, query)


class PGCraftMaterializedView:
    """Create a materialized view with an auto-generated refresh.

    After construction, ``self.table`` is a joinable
    SQLAlchemy ``Table`` whose columns mirror the query.

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
        self.table = _table_from_query(name, schema, query)
