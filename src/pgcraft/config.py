"""Global pgcraft configuration."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pgcraft.extension import PGCraftExtension
    from pgcraft.plugin import Plugin


@dataclass
class PGCraftConfig:
    """Global plugin and extension registry.

    Plugins registered here are prepended to every factory's resolved
    plugin list, so they run before factory-specific plugins.

    Extensions bundle plugins, metadata hooks, Alembic hooks, and CLI
    commands into a single unit.

    Example::

        from pgcraft.extensions.postgrest import (
            PostgRESTExtension,
        )

        config = PGCraftConfig()
        config.use(PostgRESTExtension())
        config.register(TimestampPlugin(), TenantPlugin())

        PGCraftSimple(
            "users", "public", metadata, ..., config=config
        )

    Args:
        plugins: Global plugins prepended to every factory.
        extensions: Manually registered extension instances.
        auto_discover: Whether to discover extensions via
            entry points.  Defaults to ``True``.
        utility_schema: PostgreSQL schema for pgcraft-managed
            utility functions (e.g. ``ledger_apply_state``).
            Defaults to ``"pgcraft"``.  Override only if your
            project already uses a schema named ``"pgcraft"``.

    """

    plugins: list[Plugin] = field(default_factory=list)
    extensions: list[PGCraftExtension] = field(default_factory=list)
    auto_discover: bool = True
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

    def use(self, *extensions: PGCraftExtension) -> PGCraftConfig:
        """Register one or more extensions.

        Args:
            *extensions: Extension instances to add.

        Returns:
            ``self`` for chaining.

        """
        self.extensions.extend(extensions)
        return self

    def _resolved_extensions(self) -> list[PGCraftExtension]:
        """Return manual + discovered extensions, deduped by name.

        Manual extensions take precedence over discovered ones
        with the same name.

        Returns:
            Ordered list of extension instances.

        """
        from pgcraft.extension import (  # noqa: PLC0415
            discover_extensions,
            validate_extension_deps,
        )

        seen: dict[str, PGCraftExtension] = {}
        for ext in self.extensions:
            seen[ext.name] = ext

        if self.auto_discover:
            for name, ext_cls in discover_extensions().items():
                if name not in seen:
                    seen[name] = ext_cls(name=name)

        result = list(seen.values())
        validate_extension_deps(result)
        return result

    @property
    def all_plugins(self) -> list[Plugin]:
        """Return extension plugins + direct plugins.

        Extension plugins are prepended before direct plugins.

        Returns:
            Combined plugin list.

        """
        ext_plugins: list[Plugin] = []
        for ext in self._resolved_extensions():
            ext_plugins.extend(ext.plugins())
        return ext_plugins + self.plugins
