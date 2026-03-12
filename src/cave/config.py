"""Global cave configuration."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from cave.plugin import Plugin


@dataclass
class CaveConfig:
    """Global plugin registry applied to every factory that references it.

    Plugins registered here are prepended to every factory's resolved
    plugin list, so they run before factory-specific plugins.

    Example::

        cave = CaveConfig()
        cave.register(TimestampPlugin(), TenantPlugin())

        SimpleDimensionFactory("users", "public", metadata, ..., cave=cave)
        AppendOnlyDimensionFactory("events", "public", metadata, ..., cave=cave)
    """

    plugins: list[Plugin] = field(default_factory=list)

    def register(self, *plugins: Plugin) -> CaveConfig:
        """Register one or more plugins globally.

        Args:
            *plugins: Plugin instances to add.

        Returns:
            ``self`` for chaining.

        """
        self.plugins.extend(plugins)
        return self
