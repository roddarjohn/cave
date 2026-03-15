"""LedgerActions: generate PostgreSQL functions for ledger events."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pgcraft.plugins.ledger_actions import LedgerActionsPlugin

if TYPE_CHECKING:
    from pgcraft.factory.ledger import PGCraftLedger
    from pgcraft.ledger.events import LedgerEvent


class LedgerActions:
    """Generate PostgreSQL functions for ledger events.

    Each :class:`~pgcraft.ledger.events.LedgerEvent` becomes a
    PostgreSQL function that inserts rows into the ledger through
    the API view.

    Args:
        source: A :class:`~pgcraft.factory.ledger.PGCraftLedger`
            instance.
        events: List of :class:`~pgcraft.ledger.events.LedgerEvent`
            instances to generate functions for.

    """

    def __init__(
        self,
        source: PGCraftLedger,
        events: list[LedgerEvent],
    ) -> None:
        """Create and register event functions."""
        ctx = source.ctx
        plugin = LedgerActionsPlugin(events)
        plugin.run(ctx)
