"""Ledger resource factory convenience class."""

from typing import ClassVar

from pgcraft.factory.base import ResourceFactory
from pgcraft.plugin import Plugin
from pgcraft.plugins.api import APIPlugin
from pgcraft.plugins.created_at import CreatedAtPlugin
from pgcraft.plugins.entry_id import UUIDEntryIDPlugin
from pgcraft.plugins.ledger import (
    LedgerTablePlugin,
    LedgerTriggerPlugin,
)
from pgcraft.plugins.pk import SerialPKPlugin


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

    Pass ``plugins=[...]`` to replace these entirely, or
    ``extra_plugins=[...]`` to append to them.
    """

    DEFAULT_PLUGINS: ClassVar[list[Plugin]] = [
        SerialPKPlugin(),
        UUIDEntryIDPlugin(),
        CreatedAtPlugin(),
        LedgerTablePlugin(),
        APIPlugin(grants=["select", "insert"]),
        LedgerTriggerPlugin(),
    ]
