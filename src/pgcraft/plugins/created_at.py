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
    for all consumers, and optionally appends a
    :class:`~sqlalchemy.Column` to ``ctx.injected_columns``.

    Set ``inject=True`` (the default) for table plugins like
    ``LedgerTablePlugin`` that consume ``ctx.injected_columns``
    directly.  Set ``inject=False`` when the downstream table plugin
    builds its own ``created_at`` column from the name alone (e.g.
    ``AppendOnlyTablePlugin``, ``EAVTablePlugin``).

    Args:
        column_name: Name of the timestamp column
            (default ``"created_at"``).
        inject: Whether to append the column to
            ``ctx.injected_columns`` (default ``True``).

    """

    def __init__(
        self,
        column_name: str = "created_at",
        *,
        inject: bool = True,
    ) -> None:
        """Store the column name and injection flag."""
        self._column_name = column_name
        self._inject = inject

    def run(self, ctx: FactoryContext) -> None:
        """Store the column name and optionally inject the column."""
        ctx["created_at_column"] = self._column_name
        if self._inject:
            ctx.injected_columns.append(
                Column(
                    self._column_name,
                    DateTime(timezone=True),
                    server_default="now()",
                )
            )
