"""Plugin that adds a created_at timestamp column."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import Column, DateTime

if TYPE_CHECKING:
    from pgcraft.factory.context import FactoryContext

from pgcraft.plugin import Plugin, produces


@produces("created_at_column")
class CreatedAtPlugin(Plugin):
    """Provide a ``created_at`` timestamp column for table plugins.

    Stores the column name as a string in ``ctx["created_at_column"]``
    (for plugins like ``AppendOnlyTablePlugin`` and ``EAVTablePlugin``
    that build their own column) **and** appends a
    :class:`~sqlalchemy.Column` to ``ctx.injected_columns`` (for
    plugins like ``LedgerTablePlugin`` that consume injected columns
    directly).

    Args:
        column_name: Name of the timestamp column
            (default ``"created_at"``).

    """

    def __init__(self, column_name: str = "created_at") -> None:
        """Store the column name."""
        self._column_name = column_name

    def run(self, ctx: FactoryContext) -> None:
        """Store the column name and inject the column."""
        ctx["created_at_column"] = self._column_name
        ctx.injected_columns.append(
            Column(
                self._column_name,
                DateTime(timezone=True),
                server_default="now()",
            )
        )
