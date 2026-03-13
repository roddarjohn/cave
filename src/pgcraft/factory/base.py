"""Core ResourceFactory: plugin runner."""

from __future__ import annotations

from graphlib import CycleError, TopologicalSorter
from typing import TYPE_CHECKING, ClassVar

from pgcraft.errors import PGCraftValidationError
from pgcraft.factory.context import FactoryContext
from pgcraft.validator import validate_schema_items

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy import MetaData
    from sqlalchemy.schema import SchemaItem

    from pgcraft.check import PGCraftCheck
    from pgcraft.plugin import Plugin
    from pgcraft.statistics import PGCraftStatisticsView


def _resolve_plugins(
    config: object | None,  # PGCraftConfig, avoiding circular import
    plugins: list[Plugin] | None,
    extra_plugins: list[Plugin] | None,
    defaults: list[Plugin],
) -> list[Plugin]:
    """Resolve the effective plugin list for a factory invocation.

    Args:
        config: Optional :class:`~pgcraft.config.PGCraftConfig` providing
            global plugins.
        plugins: If given, replaces ``defaults``.  If ``None``,
            ``defaults`` is used.
        extra_plugins: Always appended to the resolved list.
        defaults: The factory's ``DEFAULT_PLUGINS``.

    Returns:
        Ordered list of plugins to run.

    """
    global_plugins: list[Plugin] = getattr(config, "plugins", [])
    factory_plugins = plugins if plugins is not None else list(defaults)
    local = extra_plugins or []
    return global_plugins + factory_plugins + local


def _run_plugin_validators(
    plugins: list[Plugin],
) -> None:
    """Collect and run all class-level validators, deduped by id.

    Args:
        plugins: Resolved plugin list to validate.

    """
    seen: set[int] = set()
    for p in plugins:
        validators: list[Callable[[list[Plugin]], None]] = getattr(
            type(p), "_validators", []
        )
        for v in validators:
            if id(v) not in seen:
                seen.add(id(v))
                v(plugins)


def _sort_plugins(plugins: list[Plugin]) -> list[Plugin]:
    """Sort plugins topologically by produces/requires declarations.

    Plugins with no declared dependencies keep their original relative
    order.  References to keys not produced by any plugin in this list
    are treated as externally satisfied and ignored for ordering.

    Args:
        plugins: The resolved plugin list to sort.

    Returns:
        A new list of the same plugins in a valid execution order.

    Raises:
        PGCraftValidationError: If a dependency cycle is detected.

    """
    # Last producer of a key wins for dependency resolution — if
    # multiple plugins produce the same key, requires-edges point
    # to the last one, meaning overriding plugins run after the
    # ones they override.
    producers: dict[str, Plugin] = {}
    for p in plugins:
        for key in p.resolved_produces():
            producers[key] = p

    # Build predecessor graph: each plugin maps to the set of
    # plugins whose output it requires.  Only edges within this
    # plugin list matter.
    graph: dict[Plugin, set[Plugin]] = {p: set() for p in plugins}
    for p in plugins:
        for key in p.resolved_requires():
            producer = producers.get(key)
            if producer is not None:
                graph[p].add(producer)

    # Use original list index as tiebreaker so unrelated plugins
    # preserve their declared order.
    original_order = {id(p): i for i, p in enumerate(plugins)}
    ts = TopologicalSorter(graph)
    try:
        ts.prepare()
    except CycleError as exc:
        names = ", ".join(type(p).__name__ for p in exc.args[1])
        msg = f"Circular plugin dependency detected among: {names}"
        raise PGCraftValidationError(msg) from exc

    result: list[Plugin] = []
    while ts.is_active():
        ready = sorted(
            ts.get_ready(),
            key=lambda p: original_order[id(p)],
        )
        for p in ready:
            result.append(p)
            ts.done(p)

    return result


class ResourceFactory:
    """Core factory: resolves plugins and runs them in dependency order.

    Subclasses declare ``DEFAULT_PLUGINS`` to establish their
    standard behaviour.  Callers can override or extend the plugin
    list via ``plugins`` / ``extra_plugins``, and inject global
    plugins via ``config``.

    Plugin execution order is determined by each plugin's
    :attr:`~pgcraft.plugin.Plugin.produces` and
    :attr:`~pgcraft.plugin.Plugin.requires` declarations.  Plugins
    with no declared dependencies run in the order they appear in
    the list.

    Example::

        class SimpleDimensionResourceFactory(ResourceFactory):
            DEFAULT_PLUGINS: ClassVar[list[Plugin]] = [
                SerialPKPlugin(),
                SimpleTablePlugin(),
                APIPlugin(),
                SimpleTriggerPlugin(),
            ]

    Args:
        tablename: Name of the dimension table.
        schemaname: PostgreSQL schema for all generated objects.
        metadata: SQLAlchemy ``MetaData`` the objects are bound to.
        schema_items: Column and constraint definitions.  Must not
            include a primary key column.
        config: Optional global config supplying prepended plugins.
        plugins: If given, replaces ``DEFAULT_PLUGINS`` entirely.
        extra_plugins: Appended to the resolved plugin list.

    Raises:
        PGCraftValidationError: If any schema item fails validation,
            two plugins share a singleton group, two plugins produce
            the same ctx key, or a plugin dependency cycle is
            detected.

    """

    DEFAULT_PLUGINS: ClassVar[list[Plugin]] = []

    def __init__(  # noqa: PLR0913
        self,
        tablename: str,
        schemaname: str,
        metadata: MetaData,
        schema_items: list[SchemaItem | PGCraftCheck | PGCraftStatisticsView],
        *,
        config: object | None = None,
        plugins: list[Plugin] | None = None,
        extra_plugins: list[Plugin] | None = None,
    ) -> None:
        """Create the dimension and register it on *metadata*."""
        validate_schema_items(schema_items)

        resolved = _resolve_plugins(
            config, plugins, extra_plugins, self.DEFAULT_PLUGINS
        )
        _run_plugin_validators(resolved)

        ctx = FactoryContext(
            tablename=tablename,
            schemaname=schemaname,
            metadata=metadata,
            schema_items=list(schema_items),
            plugins=resolved,
        )

        for p in _sort_plugins(resolved):
            p.run(ctx)
