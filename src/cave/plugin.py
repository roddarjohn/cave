"""Plugin base class and decorators for cave factory extensions."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, TypeVar

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy import Column

    from cave.factory.context import FactoryContext

_PluginT = TypeVar("_PluginT", bound="type[Plugin]")


def singleton(group: str) -> Callable[[_PluginT], _PluginT]:
    """Declare that at most one plugin of *group* may appear in a plugin list.

    The factory raises :class:`~cave.errors.CaveValidationError` at
    construction time if two plugins with the same group name are
    present in the resolved plugin list.

    Example::

        @singleton("__pk__")
        class MyPKPlugin(Plugin):
            ...

    Args:
        group: Arbitrary group identifier.  By convention, built-in
            groups use dunder names (``"__pk__"``, ``"__table__"``).

    Returns:
        A class decorator that sets ``singleton_group`` on the class.

    """

    def decorator(cls: _PluginT) -> _PluginT:
        cls.singleton_group = group
        return cls

    return decorator


class Plugin:
    """Base class for cave factory plugins.

    Each plugin can implement any subset of the lifecycle hooks.
    All methods are no-ops by default.

    Lifecycle order per factory invocation:

    1. ``pk_columns`` -- first non-None result across all plugins
       is used as the PK column list.
    2. ``extra_columns`` -- all results are concatenated and
       prepended to the dimension list in ``ctx.extra_columns``.
    3. ``create_tables`` -- all plugins called in order.
    4. ``create_views`` -- all plugins called in order.
    5. ``create_triggers`` -- all plugins called in order.
    6. ``post_create`` -- all plugins called in order.

    Plugins communicate through ``ctx`` using string keys
    (``ctx["key"] = value`` / ``ctx["key"]`` / ``"key" in ctx``).
    The keys a plugin reads and writes are explicit constructor
    arguments with sensible defaults, so multiple independent pipelines
    can coexist in one factory by using distinct keys.  Ordering within
    each phase is determined by the plugin list: if plugin B reads what
    plugin A writes, A must appear before B.

    Use the :func:`singleton` decorator to declare that at most one
    plugin of a given group may appear in any resolved plugin list.
    """

    singleton_group: ClassVar[str | None] = None

    def pk_columns(self, _ctx: FactoryContext) -> list[Column] | None:
        """Return PK column(s) for the root table.

        The first non-None result across all plugins is used.
        Return ``None`` to defer to the next plugin.

        Args:
            _ctx: The factory context.

        Returns:
            A list of primary key columns, or ``None`` to skip.

        """
        return None

    def extra_columns(self, _ctx: FactoryContext) -> list[Column]:
        """Return additional columns to include before the user dimensions.

        Results from all plugins are concatenated and stored in
        ``ctx.extra_columns`` before ``create_tables`` is called.

        Args:
            _ctx: The factory context.

        Returns:
            A (possibly empty) list of extra columns.

        """
        return []

    def create_tables(self, ctx: FactoryContext) -> None:
        """Create backing tables for this dimension.

        Write created tables into ``ctx`` under the keys declared in
        this plugin's constructor.

        Args:
            ctx: The factory context with resolved pk/extra columns.

        """

    def create_views(self, ctx: FactoryContext) -> None:
        """Create views on top of the backing tables.

        Read objects from ``ctx`` and write created views and/or proxy
        tables back into ``ctx`` under the keys declared in this
        plugin's constructor.

        Args:
            ctx: The factory context after table creation.

        """

    def create_triggers(self, ctx: FactoryContext) -> None:
        """Create INSTEAD OF trigger functions and register them.

        Args:
            ctx: The factory context after view creation.

        """

    def post_create(self, ctx: FactoryContext) -> None:
        """Run the final lifecycle hook.

        Used for API resource registration, custom metadata, etc.

        Args:
            ctx: The fully populated factory context.

        """
