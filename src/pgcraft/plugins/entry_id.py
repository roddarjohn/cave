"""Entry ID plugin for correlating related ledger entries."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import Column, text
from sqlalchemy.dialects.postgresql import UUID

if TYPE_CHECKING:
    from pgcraft.factory.context import FactoryContext

from pgcraft.plugin import Plugin, produces, singleton


@produces("entry_id_column")
@singleton("__entry_id__")
class UUIDEntryIDPlugin(Plugin):
    """Provide a UUIDv4 entry ID column for ledger tables.

    Stores a :class:`~sqlalchemy.Column` in ``ctx["entry_id_column"]``
    that downstream table plugins (e.g.
    :class:`~pgcraft.plugins.ledger.LedgerTablePlugin`) splice into
    the table definition.  The column uses PostgreSQL's
    ``gen_random_uuid()`` as a server default so callers can omit
    it for single-entry inserts while still providing an explicit
    value to correlate multi-row entries.

    Args:
        column_name: Name of the entry ID column
            (default ``"entry_id"``).

    """

    def __init__(self, column_name: str = "entry_id") -> None:
        """Store the column name."""
        self._column_name = column_name

    def run(self, ctx: FactoryContext) -> None:
        """Store the entry ID column in the ctx store."""
        ctx["entry_id_column"] = Column(
            self._column_name,
            UUID(as_uuid=True),
            nullable=False,
            server_default=text("gen_random_uuid()"),
        )
