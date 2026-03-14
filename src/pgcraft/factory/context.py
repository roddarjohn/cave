"""Factory context dataclass."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from sqlalchemy import Column

from pgcraft.check import PGCraftCheck
from pgcraft.statistics import PGCraftStatisticsView

if TYPE_CHECKING:
    from sqlalchemy import MetaData
    from sqlalchemy.schema import SchemaItem

    from pgcraft.columns import PrimaryKeyColumns
    from pgcraft.plugin import Plugin

_MISSING = object()


@dataclass
class FactoryContext:
    """Carries inputs and accumulates plugin outputs.

    **Typed input fields** (set by the factory, read-only for plugins):

    - ``tablename``, ``schemaname``, ``metadata``, ``schema_items``,
      ``plugins`` -- as passed to the factory constructor.

    **Plugin store** (read/write via item syntax):

    Plugins communicate by storing and retrieving arbitrary values
    using string keys.  The key names a plugin reads and writes are
    explicit constructor arguments on that plugin (with sensible
    defaults), so multiple independent pipelines can coexist by using
    distinct keys.

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

    **Injected columns** (append-only list):

    Plugins that provide columns for table construction (e.g.
    ``CreatedAtPlugin``, ``UUIDEntryIDPlugin``, ``DoubleEntryPlugin``)
    append :class:`~sqlalchemy.Column` objects to
    ``ctx.injected_columns``.  Table plugins spread this list into
    the table definition alongside PK and dimension columns.
    """

    tablename: str
    schemaname: str
    metadata: MetaData
    schema_items: list[SchemaItem | PGCraftCheck | PGCraftStatisticsView]
    plugins: list[Plugin]

    _store: dict[str, Any] = field(default_factory=dict, init=False, repr=False)
    injected_columns: list[Column] = field(
        default_factory=list, init=False, repr=False
    )

    @property
    def columns(self) -> list[Column]:
        """Return only ``Column`` instances from schema_items.

        Useful when a plugin needs to iterate over column
        definitions (e.g. to extract column names or types).
        """
        return [item for item in self.schema_items if isinstance(item, Column)]

    @property
    def dim_column_names(self) -> list[str]:
        """Return writable (non-PK, non-computed) column names.

        Filters out primary-key and computed columns from
        ``schema_items``, leaving only the user-defined
        dimension columns that a plugin should read or write.
        Equivalent to the ``_dim_column_names`` helper that was
        previously duplicated across multiple plugin modules.
        """
        return [
            col.key
            for col in self.columns
            if not col.primary_key and not col.computed
        ]

    @property
    def pk_column_name(self) -> str:
        """Return the primary key column name.

        Shorthand for ``ctx["pk_columns"].first_key``.  Requires
        a PK plugin (e.g. ``SerialPKPlugin``) to have run first.

        Raises:
            KeyError: If ``pk_columns`` has not been set yet.

        """
        pk_columns: PrimaryKeyColumns = self["pk_columns"]
        return pk_columns.first_key

    @property
    def table_items(self) -> list[SchemaItem]:
        """Return schema items suitable for table creation.

        Filters out :class:`~pgcraft.check.PGCraftCheck` (which are
        handled by dedicated check plugins) but keeps all real
        SQLAlchemy ``SchemaItem`` objects: columns, constraints,
        indexes, computed columns, etc.
        """
        return [
            item
            for item in self.schema_items
            if not isinstance(
                item,
                (PGCraftCheck, PGCraftStatisticsView),
            )
        ]

    def __getitem__(self, key: str) -> Any:  # noqa: ANN401
        """Return the value stored under *key*.

        Args:
            key: The store key to look up.

        Raises:
            KeyError: With a plugin-ordering hint when *key* is
                absent.

        """
        try:
            return self._store[key]
        except KeyError:
            set_keys = sorted(self._store)
            msg = (
                f"ctx[{key!r}] has not been set. "
                f"The plugin that writes {key!r} must appear "
                f"before this one in the plugin list. "
                f"Keys set so far: {set_keys}"
            )
            raise KeyError(msg) from None

    def __setitem__(
        self,
        key: str,
        value: Any,  # noqa: ANN401
    ) -> None:
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
        """Store *value* under *key*, with optional override.

        Args:
            key: The store key to write.
            value: The value to store.
            force: If ``True``, overwrite an existing value without
                raising.  Use this when a plugin intentionally
                replaces a previous plugin's output.

        Raises:
            KeyError: If *key* is already set and ``force`` is
                ``False``.

        """
        if not force and key in self._store:
            msg = f"ctx[{key!r}] is already set. Pass force=True to override."
            raise KeyError(msg)
        self._store[key] = value
