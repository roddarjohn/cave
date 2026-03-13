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

        SimpleDimensionResourceFactory(
            "users", "public", metadata, ..., config=config
        )
        AppendOnlyDimensionResourceFactory(
            "events", "public", metadata, ..., config=config
        )
    """

    plugins: list[Plugin] = field(default_factory=list)

    def register(self, *plugins: Plugin) -> PGCraftConfig:
        """Register one or more plugins globally.

        Args:
            *plugins: Plugin instances to add.

        Returns:
            ``self`` for chaining.

        """
        self.plugins.extend(plugins)
        return self
