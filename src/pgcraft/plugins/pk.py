"""Primary key plugins."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import Column, Integer

if TYPE_CHECKING:
    from pgcraft.factory.context import FactoryContext

from pgcraft.columns import PrimaryKeyColumns
from pgcraft.plugin import Plugin, produces, singleton


@produces("pk_columns")
@singleton("__pk__")
class SerialPKPlugin(Plugin):
    """Provide an auto-increment integer primary key column.

    Args:
        column_name: Name of the PK column (default ``"id"``).

    """

    def __init__(self, column_name: str = "id") -> None:
        """Store the PK column name."""
        self._column_name = column_name

    def run(self, ctx: FactoryContext) -> None:
        """Store a PrimaryKeyColumns in the ctx store."""
        ctx["pk_columns"] = PrimaryKeyColumns(
            [Column(self._column_name, Integer, primary_key=True)]
        )
