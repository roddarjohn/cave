"""Plugin that provides a created_at timestamp column name."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pgcraft.factory.context import FactoryContext

from pgcraft.plugin import Plugin, produces


@produces("created_at_column")
class CreatedAtPlugin(Plugin):
    """Provide the ``created_at`` column name for table plugins.

    Stores the column name as a string in ``ctx["created_at_column"]``.
    Each table plugin that needs a ``created_at`` column is responsible
    for constructing it from this name (see ``AppendOnlyTablePlugin``,
    ``EAVTablePlugin``, ``LedgerTablePlugin``).

    Args:
        column_name: Name of the timestamp column
            (default ``"created_at"``).

    """

    def __init__(self, column_name: str = "created_at") -> None:
        """Store the column name."""
        self._column_name = column_name

    def run(self, ctx: FactoryContext) -> None:
        """Store the column name in ctx."""
        ctx["created_at_column"] = self._column_name
