"""Plugin base class and helpers for cave factory extensions."""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import TYPE_CHECKING, ClassVar, TypeVar

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy import Column

    from cave.factory.context import FactoryContext

_PluginT = TypeVar("_PluginT", bound="type[Plugin]")


@dataclass(frozen=True)
class Dynamic:
    """Reference to an instance attribute that holds the actual ctx key.

    Use inside :func:`produces` / :func:`requires` decorators when the
    ctx key name is determined at construction time rather than being a
    fixed string::

        @produces(Dynamic("table_key"))
        @requires("raw")
        class MyPlugin(Plugin):
            def __init__(self, table_key: str = "result") -> None:
                self.table_key = table_key

            def run(self, ctx: FactoryContext) -> None:
                ctx[self.table_key] = build(ctx["raw"])

    The factory resolves each ``Dynamic`` via ``getattr(instance, attr)``
    when building the dependency graph.
    """

    attr: str


def _validate_dynamic_keys(
    cls: type, keys: tuple[str | Dynamic, ...], decorator_name: str
) -> None:
    """Raise TypeError if any Dynamic attr is not an __init__ parameter.

    Args:
        cls: The plugin class being decorated.
        keys: The key arguments passed to the decorator.
        decorator_name: ``"produces"`` or ``"requires"`` for the error message.

    Raises:
        TypeError: If a Dynamic attr name is not an ``__init__`` kwarg.

    """
    init = cls.__dict__.get("__init__")
    if init is None:
        return
    params = set(inspect.signature(init).parameters) - {"self"}
    for key in keys:
        if isinstance(key, Dynamic) and key.attr not in params:
            msg = (
                f"@{decorator_name}(Dynamic({key.attr!r})) on {cls.__name__} "
                f"references an attribute that is not an __init__ parameter. "
                f"Available parameters: {sorted(params)}"
            )
            raise TypeError(msg)


def produces(*keys: str | Dynamic) -> Callable[[_PluginT], _PluginT]:
    """Declare the ctx keys this plugin's ``run`` method writes.

    Applied as a class decorator, alongside :func:`requires` and
    :func:`singleton`::

        @produces(Dynamic("table_key"))
        class MyTablePlugin(Plugin):
            ...

    Args:
        *keys: Ctx key strings or :class:`Dynamic` references to
            instance attributes that hold the actual key names.

    Returns:
        A class decorator that attaches ``_produces`` to the class.

    Raises:
        TypeError: If a Dynamic attr name is not an ``__init__`` parameter.

    """

    def decorator(cls: _PluginT) -> _PluginT:
        _validate_dynamic_keys(cls, keys, "produces")
        cls._produces = list(keys)
        return cls

    return decorator


def requires(*keys: str | Dynamic) -> Callable[[_PluginT], _PluginT]:
    """Declare the ctx keys this plugin's ``run`` method reads.

    Applied as a class decorator, alongside :func:`produces` and
    :func:`singleton`::

        @requires(Dynamic("table_key"), "schema_info")
        class MyViewPlugin(Plugin):
            ...

    Args:
        *keys: Ctx key strings or :class:`Dynamic` references to
            instance attributes that hold the actual key names.

    Returns:
        A class decorator that attaches ``_requires`` to the class.

    Raises:
        TypeError: If a Dynamic attr name is not an ``__init__`` parameter.

    """

    def decorator(cls: _PluginT) -> _PluginT:
        _validate_dynamic_keys(cls, keys, "requires")
        cls._requires = list(keys)
        return cls

    return decorator


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

    Lifecycle per factory invocation:

    1. ``pk_columns`` -- first non-None result across all plugins is
       used as the PK column list.
    2. ``extra_columns`` -- all results are concatenated and stored in
       ``ctx.extra_columns``.
    3. ``run`` -- called once per plugin, in dependency order.  Execution
       order is determined by topological sort using the :func:`produces`
       and :func:`requires` class decorators.

    Declaring dependencies::

        @produces(Dynamic("out_key"))
        @requires("primary")
        class MyPlugin(Plugin):
            def __init__(self, out_key: str = "result") -> None:
                self.out_key = out_key

            def run(self, ctx: FactoryContext) -> None:
                ctx[self.out_key] = transform(ctx["primary"])

    Plugins communicate through ``ctx`` using string keys.
    Use the :func:`singleton` decorator to declare that at most one
    plugin of a given group may appear in any resolved plugin list.
    """

    singleton_group: ClassVar[str | None] = None
    _produces: ClassVar[list[str | Dynamic]] = []
    _requires: ClassVar[list[str | Dynamic]] = []

    def resolved_produces(self) -> list[str]:
        """Return the ctx keys this plugin writes, with Dynamic refs resolved.

        Reads the ``_produces`` list set by the :func:`produces` decorator
        and substitutes each :class:`Dynamic` with ``getattr(self, attr)``.

        Returns:
            List of ctx key strings this plugin will write to.

        """
        return [
            getattr(self, k.attr) if isinstance(k, Dynamic) else k
            for k in type(self)._produces  # noqa: SLF001
        ]

    def resolved_requires(self) -> list[str]:
        """Return the ctx keys this plugin reads, with Dynamic refs resolved.

        Reads the ``_requires`` list set by the :func:`requires` decorator
        and substitutes each :class:`Dynamic` with ``getattr(self, attr)``.

        Returns:
            List of ctx key strings this plugin expects to already be set.

        """
        return [
            getattr(self, k.attr) if isinstance(k, Dynamic) else k
            for k in type(self)._requires  # noqa: SLF001
        ]

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
        ``ctx.extra_columns`` before ``run`` is called.

        Args:
            _ctx: The factory context.

        Returns:
            A (possibly empty) list of extra columns.

        """
        return []

    def run(self, ctx: FactoryContext) -> None:
        """Execute this plugin's work against *ctx*.

        The factory calls this once per plugin, after all plugins have
        had their :meth:`pk_columns` and :meth:`extra_columns` collected
        and after topological sorting by :func:`produces` / :func:`requires`.

        Args:
            ctx: The factory context with resolved pk/extra columns.

        """
