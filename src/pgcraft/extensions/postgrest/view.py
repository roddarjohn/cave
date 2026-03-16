"""PostgRESTView: view + grants + triggers."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pgcraft.extensions.postgrest.plugin import (
    PostgRESTPlugin,
    _resolve_included_columns,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy import Table
    from sqlalchemy.sql.expression import Select

    from pgcraft.factory.base import ResourceFactory
    from pgcraft.plugin import Plugin
    from pgcraft.resource import Grant


class PostgRESTView:
    """Create a PostgREST API view with grants and triggers.

    Wraps :class:`PostgRESTPlugin` to create the view and
    resource, then runs any additional plugins (typically
    trigger plugins).

    Writable columns are resolved from ``columns`` /
    ``exclude_columns`` / ``query`` and stored as
    ``ctx["writable_columns"]`` so trigger plugins can
    pick them up automatically.

    Args:
        source: A :class:`~pgcraft.factory.base.ResourceFactory`
            instance whose table/context to expose.
        schema: Schema for the API view (default ``"api"``).
        grants: PostgREST privileges (default ``["select"]``).
        query: Optional callable ``(query, source_table) ->
            Select`` for SQLAlchemy-style view customization.
        plugins: Plugins to run after the view is created
            (e.g. trigger plugins).
        columns: Column names to include.  Mutually exclusive
            with ``exclude_columns``.
        exclude_columns: Column names to exclude.  Mutually
            exclusive with ``columns``.

    """

    def __init__(  # noqa: PLR0913
        self,
        source: ResourceFactory,
        schema: str = "api",
        grants: list[Grant] | None = None,
        query: Callable[[Select, Table], Select] | None = None,
        *,
        plugins: list[Plugin] | None = None,
        columns: list[str] | None = None,
        exclude_columns: list[str] | None = None,
    ) -> None:
        """Create the API view, register grants, run plugins."""
        self.source = source
        self.schema = schema
        self.grants: list[Grant] = grants if grants is not None else ["select"]
        ctx = source.ctx

        view_plugin = PostgRESTPlugin(
            schema=schema,
            grants=self.grants,
            columns=columns,
            exclude_columns=exclude_columns,
            query=query,
        )
        view_plugin.run(ctx)
        self.view = ctx["api"]

        # Resolve writable columns so trigger plugins pick
        # them up via ctx["writable_columns"].
        if "primary" in ctx:
            primary = ctx["primary"]
            effective = _resolve_included_columns(
                primary, columns, exclude_columns
            )
            if effective is not None:
                dim_set = set(ctx.dim_column_names)
                writable = [c for c in effective if c in dim_set]
                ctx.set(
                    "writable_columns",
                    writable,
                    force=True,
                )
            elif query is not None:
                ctx.set(
                    "writable_columns",
                    list(ctx.dim_column_names),
                    force=True,
                )

        for p in plugins or []:
            p.run(ctx)
