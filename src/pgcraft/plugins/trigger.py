"""Generic INSTEAD OF trigger plugin."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from pgcraft.plugin import Dynamic, Plugin, requires
from pgcraft.utils.trigger import (
    collect_trigger_views,
    register_view_triggers,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from pgcraft.factory.context import FactoryContext


@dataclass
class TriggerOp:
    """A single INSTEAD OF trigger operation.

    Args:
        name: DML operation name (``"insert"``, ``"update"``,
            ``"delete"``).
        body: Pre-rendered PL/pgSQL function body.

    """

    name: str
    body: str


@requires(Dynamic("view_key"))
class InsteadOfTriggerPlugin(Plugin):
    """Register INSTEAD OF triggers from pre-rendered PL/pgSQL bodies.

    A generic, composable trigger plugin.  Each factory type
    provides an ``ops_builder`` callable that reads from the
    factory context and returns a list of :class:`TriggerOp`
    with fully rendered PL/pgSQL.

    Args:
        ops_builder: Callable that takes a
            :class:`~pgcraft.factory.context.FactoryContext` and
            returns a list of :class:`TriggerOp`.
        naming_defaults: Default naming templates for function
            and trigger names.
        function_key: Key for function name resolution.
        trigger_key: Key for trigger name resolution.
        view_key: Key in ``ctx`` for the trigger target view
            (default ``"api"``).  If absent from ``ctx``, only
            the private dimension view gets triggers.
        permitted_operations: When set, only operations whose
            names appear in this list are registered.

    """

    def __init__(  # noqa: PLR0913
        self,
        ops_builder: Callable[[FactoryContext], list[TriggerOp]],
        naming_defaults: dict[str, str],
        function_key: str,
        trigger_key: str,
        view_key: str = "api",
        permitted_operations: list[str] | None = None,
        *,
        include_private_view: bool = True,
        extra_requires: list[str] | None = None,
    ) -> None:
        """Store configuration."""
        self.ops_builder = ops_builder
        self.naming_defaults = naming_defaults
        self.function_key = function_key
        self.trigger_key = trigger_key
        self.view_key = view_key
        self.permitted_operations = permitted_operations
        self.include_private_view = include_private_view
        self._extra_requires = extra_requires or []

    def resolved_requires(self) -> list[str]:
        """Return base requires plus extra runtime keys."""
        base = super().resolved_requires()
        return base + list(self._extra_requires)

    def _collect_views(
        self,
        ctx: FactoryContext,
    ) -> list[tuple[str, str]]:
        """Return ``(schema, fullname)`` pairs for trigger targets.

        When ``include_private_view`` is True, delegates to
        :func:`collect_trigger_views` which includes the
        dimension's private view.  Otherwise, registers only
        on the view stored at ``view_key``.
        """
        if self.include_private_view:
            return collect_trigger_views(ctx, self.view_key)
        api_view = ctx[self.view_key]
        api_schema = api_view.schema or "api"
        return [(api_schema, f"{api_schema}.{ctx.tablename}")]

    def run(self, ctx: FactoryContext) -> None:
        """Register INSTEAD OF triggers on target views.

        When ``permitted_operations`` was not set at construction
        time, falls back to ``ctx["permitted_operations"]`` if
        present.  This lets :class:`PostgRESTView` control which
        DML operations get triggers based on grants.
        """
        ops = self.ops_builder(ctx)
        allowed = self.permitted_operations
        if allowed is None and "permitted_operations" in ctx:
            allowed = ctx["permitted_operations"]
        if allowed is not None:
            ops = [o for o in ops if o.name in set(allowed)]

        rendered: list[tuple[str, str]] = [(o.name, o.body) for o in ops]
        if not rendered:
            return

        for view_schema, view_fullname in self._collect_views(ctx):
            register_view_triggers(
                metadata=ctx.metadata,
                view_schema=view_schema,
                view_fullname=view_fullname,
                tablename=ctx.tablename,
                ops=rendered,
                naming_defaults=self.naming_defaults,
                function_key=self.function_key,
                trigger_key=self.trigger_key,
            )
