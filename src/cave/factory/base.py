"""Core DimensionFactory: plugin runner."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from cave.factory.context import FactoryContext
from cave.validator import validate_schema_items

if TYPE_CHECKING:
    from sqlalchemy import MetaData
    from sqlalchemy.schema import SchemaItem

    from cave.plugin import Plugin


def _resolve_plugins(
    cave: object | None,  # CaveConfig, avoiding circular import
    plugins: list[Plugin] | None,
    extra_plugins: list[Plugin] | None,
    defaults: list[Plugin],
) -> list[Plugin]:
    """Resolve the effective plugin list for a factory invocation.

    Args:
        cave: Optional :class:`~cave.config.CaveConfig` providing
            global plugins.
        plugins: If given, replaces ``defaults``.  If ``None``,
            ``defaults`` is used.
        extra_plugins: Always appended to the resolved list.
        defaults: The factory's ``DEFAULT_PLUGINS``.

    Returns:
        Ordered list of plugins to run.

    """
    global_plugins: list[Plugin] = getattr(cave, "plugins", [])
    factory_plugins = plugins if plugins is not None else list(defaults)
    local = extra_plugins or []
    return global_plugins + factory_plugins + local


class DimensionFactory:
    """Core factory: runs plugins through the six lifecycle phases.

    Subclasses declare ``DEFAULT_PLUGINS`` to establish their
    standard behaviour.  Callers can override or extend the plugin
    list via ``plugins`` / ``extra_plugins``, and inject global
    plugins via ``cave``.

    Example::

        class SimpleDimensionFactory(DimensionFactory):
            DEFAULT_PLUGINS: ClassVar[list[Plugin]] = [
                SerialPKPlugin(),
                SimpleTablePlugin(),
                APIPlugin(),
                SimpleTriggerPlugin(),
            ]

    Args:
        tablename: Name of the dimension table.
        schemaname: PostgreSQL schema for all generated objects.
        metadata: SQLAlchemy ``MetaData`` the objects are bound to.
        dimensions: Column and constraint definitions.  Must not
            include a primary key column.
        cave: Optional global config supplying prepended plugins.
        plugins: If given, replaces ``DEFAULT_PLUGINS`` entirely.
        extra_plugins: Appended to the resolved plugin list.

    Raises:
        CaveValidationError: If any dimension fails validation.

    """

    DEFAULT_PLUGINS: ClassVar[list[Plugin]] = []

    def __init__(  # noqa: PLR0913
        self,
        tablename: str,
        schemaname: str,
        metadata: MetaData,
        dimensions: list[SchemaItem],
        cave: object | None = None,
        plugins: list[Plugin] | None = None,
        extra_plugins: list[Plugin] | None = None,
    ) -> None:
        """Create the dimension and register it on *metadata*."""
        validate_schema_items(dimensions)

        resolved = _resolve_plugins(
            cave, plugins, extra_plugins, self.DEFAULT_PLUGINS
        )

        ctx = FactoryContext(
            tablename=tablename,
            schemaname=schemaname,
            metadata=metadata,
            dimensions=list(dimensions),
            plugins=resolved,
        )

        # Phase 0: resolve PK and extra columns before any table creation.
        ctx.pk_columns = next(
            (c for p in resolved if (c := p.pk_columns(ctx)) is not None),
            [],
        )
        ctx.extra_columns = [
            col for p in resolved for col in p.extra_columns(ctx)
        ]

        for p in resolved:
            p.create_tables(ctx)
        for p in resolved:
            p.create_views(ctx)
        for p in resolved:
            p.create_triggers(ctx)
        for p in resolved:
            p.post_create(ctx)
