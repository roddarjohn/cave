"""Factory context dataclass."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sqlalchemy import Column, MetaData
    from sqlalchemy.schema import SchemaItem

    from cave.plugin import Plugin

_MISSING = object()


@dataclass
class FactoryContext:
    """Carries inputs and accumulates plugin outputs across lifecycle phases.

    **Typed input fields** (set by the factory, read-only for plugins):

    - ``tablename``, ``schemaname``, ``metadata``, ``schema_items``,
      ``plugins`` -- as passed to the factory constructor.
    - ``pk_columns`` -- resolved from ``Plugin.pk_columns`` before any
      ``run`` call.
    - ``extra_columns`` -- concatenated from all ``Plugin.extra_columns``
      calls before ``run``.

    **Plugin store** (read/write via item syntax):

    Plugins communicate by storing and retrieving arbitrary values using
    string keys.  The key names a plugin reads and writes are explicit
    constructor arguments on that plugin (with sensible defaults), so
    multiple independent pipelines can coexist by using distinct keys.

    ``ctx["key"] = value``
        Store a value.  Raises ``KeyError`` if *key* is already set --
        two plugins writing the same key is almost certainly a mistake.
        Use ``ctx.set("key", value, force=True)`` to override
        intentionally.

    ``ctx["key"]``
        Retrieve a value.  Raises ``KeyError`` with a plugin-ordering
        hint when the key is absent.

    ``"key" in ctx``
        Test whether a key has been set without raising.
    """

    tablename: str
    schemaname: str
    metadata: MetaData
    schema_items: list[SchemaItem]
    plugins: list[Plugin]

    # Resolved by ResourceFactory before run is called.
    pk_columns: list[Column] = field(default_factory=list)
    extra_columns: list[Column] = field(default_factory=list)

    _store: dict[str, Any] = field(default_factory=dict, init=False, repr=False)

    def __getitem__(self, key: str) -> Any:  # noqa: ANN401
        """Return the value stored under *key*.

        Args:
            key: The store key to look up.

        Raises:
            KeyError: With a plugin-ordering hint when *key* is absent.

        """
        try:
            return self._store[key]
        except KeyError:
            set_keys = sorted(self._store)
            msg = (
                f"ctx[{key!r}] has not been set. "
                f"The plugin that writes {key!r} must appear before this "
                f"one in the plugin list. "
                f"Keys set so far: {set_keys}"
            )
            raise KeyError(msg) from None

    def __setitem__(self, key: str, value: Any) -> None:  # noqa: ANN401
        """Store *value* under *key*, raising on collision.

        Args:
            key: The store key to write.
            value: The value to store.

        Raises:
            KeyError: If *key* is already set.  Use
                :meth:`set` with ``force=True`` to override.

        """
        if key in self._store:
            msg = (
                f"ctx[{key!r}] is already set. "
                f"If this override is intentional, use "
                f"ctx.set({key!r}, value, force=True)."
            )
            raise KeyError(msg)
        self._store[key] = value

    def __contains__(self, key: object) -> bool:
        """Return True if *key* has been stored."""
        return key in self._store

    def set(
        self,
        key: str,
        value: Any,  # noqa: ANN401
        *,
        force: bool = False,
    ) -> None:
        """Store *value* under *key*, with optional collision override.

        Args:
            key: The store key to write.
            value: The value to store.
            force: If ``True``, overwrite an existing value without
                raising.  Use this when a plugin intentionally replaces
                a previous plugin's output.

        Raises:
            KeyError: If *key* is already set and ``force`` is ``False``.

        """
        if not force and key in self._store:
            msg = f"ctx[{key!r}] is already set. Pass force=True to override."
            raise KeyError(msg)
        self._store[key] = value
