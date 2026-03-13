"""Primary key column wrapper for cave factories."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator

    from sqlalchemy import Column


class PrimaryKeyColumns:
    """Thin wrapper around a list of primary key columns.

    Provides convenient access to the first column's key (the most
    common operation) while still supporting iteration for unpacking
    into ``Table(...)`` constructors.

    Args:
        columns: The underlying primary key column list.

    """

    def __init__(self, columns: list[Column]) -> None:
        """Store the column list."""
        self._columns = columns

    @property
    def first(self) -> Column:
        """Return the first primary key column.

        Raises:
            IndexError: If there are no columns.

        """
        return self._columns[0]

    @property
    def first_key(self) -> str:
        """Return ``first.key``, defaulting to ``"id"`` when empty.

        This replaces the repeated
        ``ctx.pk_columns[0].key if ctx.pk_columns else "id"`` pattern.
        """
        if self._columns:
            return self._columns[0].key
        return "id"

    def __iter__(self) -> Iterator[Column]:
        """Yield each primary key column."""
        return iter(self._columns)

    def __len__(self) -> int:
        """Return the number of primary key columns."""
        return len(self._columns)

    def __repr__(self) -> str:
        """Return a developer-friendly representation."""
        return f"PrimaryKeyColumns({self._columns!r})"
