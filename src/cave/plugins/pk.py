"""Primary key plugins."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import Column, Integer

if TYPE_CHECKING:
    from cave.factory.context import FactoryContext

from cave.plugin import Plugin, singleton


@singleton("__pk__")
class SerialPKPlugin(Plugin):
    """Provide an auto-increment integer primary key column.

    Args:
        column_name: Name of the PK column (default ``"id"``).

    """

    def __init__(self, column_name: str = "id") -> None:
        """Store the PK column name."""
        self._column_name = column_name

    def pk_columns(self, _ctx: FactoryContext) -> list[Column]:
        """Return a single SERIAL INTEGER primary key column."""
        return [Column(self._column_name, Integer, primary_key=True)]
