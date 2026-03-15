"""Ledger resource factory."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from pgcraft.factory.base import ResourceFactory
from pgcraft.plugins.created_at import CreatedAtPlugin
from pgcraft.plugins.entry_id import UUIDEntryIDPlugin
from pgcraft.plugins.ledger import (
    LedgerTablePlugin,
    LedgerTriggerPlugin,
)

if TYPE_CHECKING:
    from sqlalchemy import MetaData
    from sqlalchemy.schema import SchemaItem

    from pgcraft.check import PGCraftCheck
    from pgcraft.plugin import Plugin
    from pgcraft.statistics import PGCraftStatisticsView


class PGCraftLedger(ResourceFactory):
    """Create a ledger: append-only table with a value column.

    Internal plugins (always present):

    1. :class:`~pgcraft.plugins.entry_id.UUIDEntryIDPlugin` --
       UUID entry ID for correlating related entries.
    2. :class:`~pgcraft.plugins.created_at.CreatedAtPlugin` --
       ``created_at`` column name.
    3. :class:`~pgcraft.plugins.ledger.LedgerTablePlugin` --
       backing table with value column.

    A :class:`~pgcraft.plugins.pk.SerialPKPlugin` is auto-added
    when no user plugin produces ``pk_columns``.

    Use :class:`~pgcraft.views.api.APIView` to expose this table
    through a PostgREST API view with INSERT triggers.
    Use :class:`~pgcraft.views.balance.BalanceView`,
    :class:`~pgcraft.views.latest.LatestView`, and
    :class:`~pgcraft.views.actions.LedgerActions` for derived
    views and event functions.

    Args:
        events: Deprecated.  Use
            :class:`~pgcraft.views.actions.LedgerActions` instead.

    """

    _INTERNAL_PLUGINS: ClassVar[list[Plugin]] = [
        UUIDEntryIDPlugin(),
        CreatedAtPlugin(),
        LedgerTablePlugin(),
    ]

    TRIGGER_PLUGIN_CLS = LedgerTriggerPlugin

    def __init__(  # noqa: PLR0913
        self,
        tablename: str,
        schemaname: str,
        metadata: MetaData,
        schema_items: list[SchemaItem | PGCraftCheck | PGCraftStatisticsView],
        *,
        events: list | None = None,
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
            events: Deprecated.  Use
                :class:`~pgcraft.views.actions.LedgerActions`.
            config: Optional global config.
            plugins: If given, replaces ``DEFAULT_PLUGINS``.
            extra_plugins: Appended to resolved plugin list.

        """
        if events:
            from pgcraft.plugins.ledger_actions import (  # noqa: PLC0415
                LedgerActionsPlugin,
            )

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
