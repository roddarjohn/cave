"""Global pgcraft configuration."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pgcraft.plugin import Plugin


@dataclass
class PGCraftConfig:
    """Global plugin registry applied to every factory that references it.

    Plugins registered here are prepended to every factory's resolved
    plugin list, so they run before factory-specific plugins.

    Example::

        config = PGCraftConfig()
        config.register(TimestampPlugin(), TenantPlugin())

        PGCraftSimple(
            "users", "public", metadata, ..., config=config
        )
        PGCraftAppendOnly(
            "events", "public", metadata, ..., config=config
        )

    Args:
        plugins: Global plugins prepended to every factory.
        utility_schema: PostgreSQL schema for pgcraft-managed
            utility functions (e.g. ``ledger_apply_state``).
            Defaults to ``"pgcraft"``.  Override only if your
            project already uses a schema named ``"pgcraft"``.

    """

    plugins: list[Plugin] = field(default_factory=list)
    utility_schema: str = "pgcraft"

    def register(self, *plugins: Plugin) -> PGCraftConfig:
        """Register one or more plugins globally.

        Args:
            *plugins: Plugin instances to add.

        Returns:
            ``self`` for chaining.

        """
        self.plugins.extend(plugins)
        return self
