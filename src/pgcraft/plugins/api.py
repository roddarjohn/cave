"""API view plugin: creates the PostgREST-facing view and resource."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import select
from sqlalchemy_declarative_extensions import View, register_view

if TYPE_CHECKING:
    from pgcraft.factory.context import FactoryContext

from pgcraft.plugin import Dynamic, Plugin, produces, requires
from pgcraft.resource import APIResource, Grant, register_api_resource
from pgcraft.utils.query import compile_query


@produces(Dynamic("view_key"))
@requires(Dynamic("table_key"))
class APIPlugin(Plugin):
    """Create a PostgREST-facing view and register its grants.

    Reads ``ctx[table_key]`` to build a ``SELECT *`` query, registers
    the resulting view, stores it as ``ctx[view_key]``, and registers
    the :class:`~pgcraft.resource.APIResource` for role/grant generation.

    Args:
        schema: Schema for the API view (default ``"api"``).
        grants: PostgREST privileges (default ``["select"]``).
        table_key: Key in ``ctx`` to read the source table or view
            proxy from (default ``"primary"``).
        view_key: Key in ``ctx`` to store the created view under
            (default ``"api"``).

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

    def run(self, ctx: FactoryContext) -> None:
        """Create the API view and register the resource."""
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

        register_api_resource(
            ctx.metadata,
            APIResource(
                name=ctx.tablename,
                schema=self.schema,
                grants=self.grants,
            ),
        )
