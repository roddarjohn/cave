"""Simple dimension resource factory."""

from typing import ClassVar

from pgcraft.factory.base import ResourceFactory
from pgcraft.plugin import Plugin
from pgcraft.plugins.check import TableCheckPlugin
from pgcraft.plugins.fk import TableFKPlugin
from pgcraft.plugins.index import TableIndexPlugin
from pgcraft.plugins.protect import RawTableProtectionPlugin
from pgcraft.plugins.simple import SimpleTablePlugin, SimpleTriggerPlugin


class PGCraftSimple(ResourceFactory):
    """Create a simple dimension: one table with optional checks.

    Internal plugins (always present):

    1. :class:`~pgcraft.plugins.simple.SimpleTablePlugin` --
       backing table.
    2. :class:`~pgcraft.plugins.check.TableCheckPlugin` --
       materializes :class:`~pgcraft.check.PGCraftCheck` items.
    3. :class:`~pgcraft.plugins.index.TableIndexPlugin` --
       materializes :class:`~pgcraft.index.PGCraftIndex` items.
    4. :class:`~pgcraft.plugins.fk.TableFKPlugin` --
       materializes :class:`~pgcraft.fk.PGCraftFK` items.

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
        TableIndexPlugin(),
        TableFKPlugin(),
        RawTableProtectionPlugin("primary"),
    ]

    TRIGGER_PLUGIN_CLS = SimpleTriggerPlugin
