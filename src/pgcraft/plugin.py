"""Plugin base class and helpers for pgcraft factory extensions."""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import TYPE_CHECKING

from pgcraft.errors import PGCraftValidationError

if TYPE_CHECKING:
    from collections.abc import Callable

    from pgcraft.factory.context import FactoryContext


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


@dataclass(frozen=True)
class PG:
    """Minimum PostgreSQL version requirement for a plugin.

    Use inside :func:`requires` to declare that a plugin needs a
    specific PostgreSQL major version::

        @requires(PG(18))
        @produces("pk_columns")
        class UUIDV7PKPlugin(Plugin):
            ...

    The factory stores the requirement as ``min_pg_version`` on the
    class.  Call :func:`check_pg_version` with the connected
    server's major version to validate before applying DDL.
    """

    version: int


def _validate_dynamic_keys(
    cls: type,
    keys: tuple[str | Dynamic | PG, ...],
    decorator_name: str,
) -> None:
    """Raise TypeError if any Dynamic attr is not an __init__ parameter.

    Args:
        cls: The plugin class being decorated.
        keys: The key arguments passed to the decorator.
        decorator_name: ``"produces"`` or ``"requires"`` for the
            error message.

    Raises:
        TypeError: If a Dynamic attr name is not an ``__init__``
            kwarg.

    """
    init = cls.__dict__.get("__init__")
    if init is None:
        return
    params = set(inspect.signature(init).parameters) - {"self"}
    for key in keys:
        if isinstance(key, Dynamic) and key.attr not in params:
            msg = (
                f"@{decorator_name}(Dynamic({key.attr!r})) "
                f"on {cls.__name__} references an attribute "
                f"that is not an __init__ parameter. "
                f"Available parameters: {sorted(params)}"
            )
            raise TypeError(msg)


def produces[T: type[Plugin]](
    *keys: str | Dynamic,
) -> Callable[[T], T]:
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
        TypeError: If a Dynamic attr name is not an ``__init__``
            parameter.

    """

    def decorator(cls: T) -> T:
        _validate_dynamic_keys(cls, keys, "produces")
        cls._produces = list(keys)
        return cls

    return decorator


def requires[T: type[Plugin]](
    *keys: str | Dynamic | PG,
) -> Callable[[T], T]:
    """Declare the ctx keys this plugin's ``run`` method reads.

    Applied as a class decorator, alongside :func:`produces` and
    :func:`singleton`.  Accepts :class:`PG` sentinels to declare
    a minimum PostgreSQL version requirement::

        @requires(PG(18), "pk_columns")
        class MyPlugin(Plugin):
            ...

    Args:
        *keys: Ctx key strings, :class:`Dynamic` references, or
            :class:`PG` version requirements.

    Returns:
        A class decorator that attaches ``_requires`` to the class
        and sets ``min_pg_version`` if any :class:`PG` sentinel
        is present.

    Raises:
        TypeError: If a Dynamic attr name is not an ``__init__``
            parameter.

    """

    def decorator(cls: T) -> T:
        _validate_dynamic_keys(cls, keys, "requires")
        ctx_keys: list[str | Dynamic] = []
        for key in keys:
            if isinstance(key, PG):
                cls.min_pg_version = key.version
            else:
                ctx_keys.append(key)
        cls._requires = ctx_keys
        return cls

    return decorator


def _validate_singletons(plugins: list[Plugin]) -> None:
    """Raise if two plugins share the same singleton group.

    Args:
        plugins: Resolved plugin list to inspect.

    Raises:
        PGCraftValidationError: When two plugins declare the same
            non-None ``singleton_group``.

    """
    seen: dict[str, str] = {}
    for plugin in plugins:
        group: str | None = getattr(plugin, "singleton_group", None)
        if group is None:
            continue
        name = type(plugin).__name__
        if group in seen:
            msg = (
                f"Plugin group {group!r} allows only one "
                f"plugin, but found both {seen[group]} "
                f"and {name}. "
                f"Remove one from the plugin list."
            )
            raise PGCraftValidationError(msg)
        seen[group] = name


def singleton[T: type[Plugin]](
    group: str,
) -> Callable[[T], T]:
    """Declare that at most one plugin of *group* may appear.

    The factory raises :class:`~pgcraft.errors.PGCraftValidationError` at
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
        A class decorator that sets ``singleton_group`` on the class
        and registers the singleton validator.

    """

    def decorator(cls: T) -> T:
        cls.singleton_group = group
        validators: list[Callable[[list[Plugin]], None]] = getattr(
            cls, "_validators", []
        )
        if _validate_singletons not in validators:
            cls._validators = [
                *validators,
                _validate_singletons,
            ]
        return cls

    return decorator


def check_pg_version(
    server_version: int,
    plugins: list[Plugin],
) -> None:
    """Raise if any plugin requires a newer PostgreSQL version.

    Call this with the server's major version (e.g.
    ``conn.dialect.server_version_info[0]``) to get an early,
    clear error instead of a cryptic "function does not exist"
    from PostgreSQL.

    Args:
        server_version: Major version of the connected server.
        plugins: The resolved plugin list to check.

    Raises:
        PGCraftValidationError: When a plugin's
            ``min_pg_version`` exceeds *server_version*.

    """
    for p in plugins:
        required: int = getattr(p, "min_pg_version", 0)
        if required > server_version:
            name = type(p).__name__
            msg = (
                f"{name} requires PostgreSQL >= {required}, "
                f"but the server is version {server_version}."
            )
            raise PGCraftValidationError(msg)


class Plugin:
    """Base class for pgcraft factory plugins.

    Each plugin implements ``run`` to perform its work.  Execution
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

    def resolved_produces(self) -> list[str]:
        """Return the ctx keys this plugin writes, with Dynamic refs resolved.

        Reads the ``_produces`` list set by the :func:`produces`
        decorator and substitutes each :class:`Dynamic` with
        ``getattr(self, attr)``.

        Returns:
            List of ctx key strings this plugin will write to.

        """
        keys: list[str | Dynamic] = getattr(type(self), "_produces", [])
        return [
            getattr(self, k.attr) if isinstance(k, Dynamic) else k for k in keys
        ]

    def resolved_requires(self) -> list[str]:
        """Return the ctx keys this plugin reads, with Dynamic refs resolved.

        Reads the ``_requires`` list set by the :func:`requires`
        decorator and substitutes each :class:`Dynamic` with
        ``getattr(self, attr)``.

        Returns:
            List of ctx key strings this plugin expects to already
            be set.

        """
        keys: list[str | Dynamic] = getattr(type(self), "_requires", [])
        return [
            getattr(self, k.attr) if isinstance(k, Dynamic) else k for k in keys
        ]

    def run(self, ctx: FactoryContext) -> None:
        """Execute this plugin's work against *ctx*.

        The factory calls this once per plugin, after topological
        sorting by :func:`produces` / :func:`requires`.

        Args:
            ctx: The factory context.

        """
