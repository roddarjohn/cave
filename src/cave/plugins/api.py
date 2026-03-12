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
    """Create the ``api.{tablename}`` view and register PostgREST grants.

    Reads ``ctx.tables["primary"]`` to build a ``SELECT *`` query,
    registers the resulting view, and stores it as
    ``ctx.views["api"]``.  ``post_create`` registers the
    ``APIResource`` for role/grant generation.

    Args:
        schema: API schema name (default ``"api"``).
        grants: PostgREST privileges (default ``["select"]``).

    """

    def __init__(
        self,
        schema: str = "api",
        grants: list[Grant] | None = None,
    ) -> None:
        """Store the API schema and grant list."""
        self.schema = schema
        self.grants: list[Grant] = grants if grants is not None else ["select"]

    def create_views(self, ctx: FactoryContext) -> None:
        """Create the API view selecting from ``ctx.tables["primary"]``."""
        primary = ctx.tables["primary"]
        query = select(
            *[col.label(col.key) for col in primary.columns]
        ).select_from(primary)
        view = View(
            ctx.tablename,
            compile_query(query),
            schema=self.schema,
        )
        register_view(ctx.metadata, view)
        ctx.views["api"] = view

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
