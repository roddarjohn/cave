"""Generic view plugin for dimension factories."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import Column, MetaData, Table
from sqlalchemy_declarative_extensions import View, register_view

from pgcraft.plugin import Dynamic, Plugin, produces

if TYPE_CHECKING:
    from collections.abc import Callable

    from pgcraft.factory.context import FactoryContext


@produces(Dynamic("primary_key"), "__root__")
class ViewPlugin(Plugin):
    """Register a view and store a proxy table in ctx.

    A generic, composable view plugin.  Each factory type provides
    a ``query_builder`` that returns the view SQL and a
    ``proxy_builder`` that returns proxy columns for downstream
    plugins.

    Args:
        query_builder: Callable ``(ctx) -> str`` returning the
            compiled SQL for the view definition.
        proxy_builder: Callable ``(ctx) -> list[Column]`` returning
            proxy columns for the view's selectable proxy.
        primary_key: Key in ``ctx`` to store the view proxy under
            (default ``"primary"``).
        extra_requires: Additional ctx keys this plugin depends on.
            Declared to the topological sorter so that upstream
            plugins run first.

    """

    def __init__(
        self,
        query_builder: Callable[[FactoryContext], str],
        proxy_builder: Callable[[FactoryContext], list[Column]],
        primary_key: str = "primary",
        extra_requires: list[str] | None = None,
    ) -> None:
        """Store configuration."""
        self.query_builder = query_builder
        self.proxy_builder = proxy_builder
        self.primary_key = primary_key
        self._extra_requires = extra_requires or []

    def resolved_requires(self) -> list[str]:
        """Return base requires plus extra runtime keys."""
        base = super().resolved_requires()
        return base + list(self._extra_requires)

    def run(self, ctx: FactoryContext) -> None:
        """Register the view and store the proxy in ctx."""
        sql = self.query_builder(ctx)
        register_view(
            ctx.metadata,
            View(
                ctx.tablename,
                sql,
                schema=ctx.schemaname,
            ),
        )
        proxy = Table(
            ctx.tablename,
            MetaData(),
            *self.proxy_builder(ctx),
            schema=ctx.schemaname,
        )
        ctx[self.primary_key] = proxy
        ctx["__root__"] = proxy
