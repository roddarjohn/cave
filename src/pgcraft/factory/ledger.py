"""Ledger resource factory convenience class."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from pgcraft.factory.base import ResourceFactory
from pgcraft.plugins.api import APIPlugin
from pgcraft.plugins.created_at import CreatedAtPlugin
from pgcraft.plugins.entry_id import UUIDEntryIDPlugin
from pgcraft.plugins.ledger import (
    LedgerTablePlugin,
    LedgerTriggerPlugin,
)
from pgcraft.plugins.ledger_actions import LedgerActionsPlugin
from pgcraft.plugins.pk import SerialPKPlugin
from pgcraft.plugins.protect import RawTableProtectionPlugin

if TYPE_CHECKING:
    from sqlalchemy import MetaData
    from sqlalchemy.schema import SchemaItem

    from pgcraft.check import PGCraftCheck
    from pgcraft.ledger.events import LedgerEvent
    from pgcraft.plugin import Plugin
    from pgcraft.statistics import PGCraftStatisticsView


class LedgerResourceFactory(ResourceFactory):
    """Create a ledger: append-only table with a value column.

    Default plugins:

    1. :class:`~pgcraft.plugins.pk.SerialPKPlugin` -- auto-increment PK.
    2. :class:`~pgcraft.plugins.entry_id.UUIDEntryIDPlugin` --
       UUID entry ID for correlating related entries.
    3. :class:`~pgcraft.plugins.created_at.CreatedAtPlugin` --
       ``created_at`` column name.
    4. :class:`~pgcraft.plugins.ledger.LedgerTablePlugin` --
       backing table with value column.
    5. :class:`~pgcraft.plugins.api.APIPlugin` -- API view +
       resource (select + insert grants).
    6. :class:`~pgcraft.plugins.ledger.LedgerTriggerPlugin` --
       INSERT INSTEAD OF trigger.
    7. :class:`~pgcraft.plugins.protect.RawTableProtectionPlugin` --
       BEFORE triggers blocking direct DML on the backing table.

    Pass ``plugins=[...]`` to replace these entirely, or
    ``extra_plugins=[...]`` to append to them.

    When ``events`` is provided, a
    :class:`~pgcraft.plugins.ledger_actions.LedgerActionsPlugin`
    is automatically appended to ``extra_plugins`` so it runs
    after the table and API view have been created.

    Args:
        events: Optional list of
            :class:`~pgcraft.ledger.events.LedgerEvent` instances
            to generate SQL functions for.

    """

    DEFAULT_PLUGINS: ClassVar[list[Plugin]] = [
        SerialPKPlugin(),
        UUIDEntryIDPlugin(),
        CreatedAtPlugin(),
        LedgerTablePlugin(),
        APIPlugin(grants=["select", "insert"]),
        LedgerTriggerPlugin(),
        RawTableProtectionPlugin("primary"),
    ]

    def __init__(  # noqa: PLR0913
        self,
        tablename: str,
        schemaname: str,
        metadata: MetaData,
        schema_items: list[SchemaItem | PGCraftCheck | PGCraftStatisticsView],
        *,
        events: list[LedgerEvent] | None = None,
        config: object | None = None,
        plugins: list[Plugin] | None = None,
        extra_plugins: list[Plugin] | None = None,
    ) -> None:
        """Create the ledger and register it on *metadata*.

        Args:
            tablename: Name of the ledger table.
            schemaname: PostgreSQL schema for generated objects.
            metadata: SQLAlchemy ``MetaData`` to register on.
            schema_items: Dimension column definitions.
            events: Optional ledger events to generate functions for.
            config: Optional global :class:`~pgcraft.config.PGCraftConfig`.
            plugins: If given, replaces ``DEFAULT_PLUGINS`` entirely.
            extra_plugins: Appended to the resolved plugin list.

        """
        if events:
            extra_plugins = [
                *(extra_plugins or []),
                LedgerActionsPlugin(events),
            ]
        super().__init__(
            tablename,
            schemaname,
            metadata,
            schema_items,
            config=config,
            plugins=plugins,
            extra_plugins=extra_plugins,
        )
