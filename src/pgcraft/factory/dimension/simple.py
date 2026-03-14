"""Simple dimension resource factory."""

from typing import ClassVar

from pgcraft.factory.base import ResourceFactory
from pgcraft.plugin import Plugin
from pgcraft.plugins.check import TableCheckPlugin
from pgcraft.plugins.simple import SimpleTablePlugin, SimpleTriggerPlugin


class PGCraftSimple(ResourceFactory):
    """Create a simple dimension: one table with optional checks.

    Internal plugins (always present):

    1. :class:`~pgcraft.plugins.simple.SimpleTablePlugin` --
       backing table.
    2. :class:`~pgcraft.plugins.check.TableCheckPlugin` --
       materializes :class:`~pgcraft.check.PGCraftCheck` items.

    A :class:`~pgcraft.plugins.pk.SerialPKPlugin` is auto-added
    when no user plugin produces ``pk_columns``.

    Use :class:`~pgcraft.views.api.APIView` to expose this table
    through a PostgREST API view with CRUD triggers.

    Args:
        tablename: Name of the dimension table.
        schemaname: PostgreSQL schema for generated objects.
        metadata: SQLAlchemy ``MetaData`` to register on.
        schema_items: Column and constraint definitions.
        plugins: Behaviour-modifying plugins (e.g.
            ``UUIDV4PKPlugin``).
        extra_plugins: Appended to the resolved plugin list.

    """

    _INTERNAL_PLUGINS: ClassVar[list[Plugin]] = [
        SimpleTablePlugin(),
        TableCheckPlugin(),
    ]

    TRIGGER_PLUGIN_CLS = SimpleTriggerPlugin
    PROTECTED_TABLE_KEYS: ClassVar[list[str]] = ["primary"]
