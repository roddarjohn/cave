"""API view plugin: creates the PostgREST-facing view and resource."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import select
from sqlalchemy_declarative_extensions import View, register_view

if TYPE_CHECKING:
    from cave.factory.context import FactoryContext

from cave.plugin import Plugin
from cave.resource import APIResource, Grant, register_api_resource
from cave.utils.query import compile_query


class APIPlugin(Plugin):
    """Create a PostgREST-facing view and register its grants.

    Reads ``ctx.tables[table_key]`` to build a ``SELECT *`` query,
    registers the resulting view, and stores it as
    ``ctx.views[view_key]``.  ``post_create`` registers the
    ``APIResource`` for role/grant generation.

    Args:
        schema: Schema for the API view (default ``"api"``).
        grants: PostgREST privileges (default ``["select"]``).
        table_key: Key in ``ctx.tables`` to read the source table
            or view proxy from (default ``"primary"``).
        view_key: Key in ``ctx.views`` to store the created view
            under (default ``"api"``).

    """

    def __init__(
        self,
        schema: str = "api",
        grants: list[Grant] | None = None,
        table_key: str = "primary",
        view_key: str = "api",
    ) -> None:
        """Store the API configuration and context keys."""
        self.schema = schema
        self.grants: list[Grant] = grants if grants is not None else ["select"]
        self.table_key = table_key
        self.view_key = view_key

    def create_views(self, ctx: FactoryContext) -> None:
        """Create the API view selecting from ``ctx[self.table_key]``."""
        primary = ctx[self.table_key]
        query = select(
            *[col.label(col.key) for col in primary.columns]
        ).select_from(primary)
        view = View(
            ctx.tablename,
            compile_query(query),
            schema=self.schema,
        )
        register_view(ctx.metadata, view)
        ctx[self.view_key] = view

    def post_create(self, ctx: FactoryContext) -> None:
        """Register the API resource for grant generation."""
        register_api_resource(
            ctx.metadata,
            APIResource(
                name=ctx.tablename,
                schema=self.schema,
                grants=self.grants,
            ),
        )
