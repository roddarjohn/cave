"""Plugin that adds a created_at timestamp column."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pgcraft.factory.context import FactoryContext

from pgcraft.plugin import Plugin, produces


@produces("created_at_column")
class CreatedAtPlugin(Plugin):
    """Provide a ``created_at`` timestamp column name for table plugins.

    Table plugins that support ``created_at`` (such as
    :class:`~pgcraft.plugins.append_only.AppendOnlyTablePlugin` and
    :class:`~pgcraft.plugins.eav.EAVTablePlugin`) read this value to
    add the column to the root/entity table.

    Args:
        column_name: Name of the timestamp column
            (default ``"created_at"``).

    """

    def __init__(self, column_name: str = "created_at") -> None:
        """Store the column name."""
        self._column_name = column_name

    def run(self, ctx: FactoryContext) -> None:
        """Store the created_at column name in the ctx store."""
        ctx["created_at_column"] = self._column_name
