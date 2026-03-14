"""Simple dimension resource factory convenience class."""

from typing import ClassVar

from pgcraft.factory.base import ResourceFactory
from pgcraft.plugin import Plugin
from pgcraft.plugins.api import APIPlugin
from pgcraft.plugins.pk import SerialPKPlugin
from pgcraft.plugins.protect import RawTableProtectionPlugin
from pgcraft.plugins.simple import SimpleTablePlugin, SimpleTriggerPlugin
from pgcraft.plugins.statistics import StatisticsViewPlugin


class SimpleDimensionResourceFactory(ResourceFactory):
    """Create a simple dimension: one table, one API view, CRUD triggers.

    Default plugins:

    1. :class:`~pgcraft.plugins.pk.SerialPKPlugin` -- auto-increment PK.
    2. :class:`~pgcraft.plugins.simple.SimpleTablePlugin` -- backing table.
    3. :class:`~pgcraft.plugins.statistics.StatisticsViewPlugin` --
       statistics views (no-op when no statistics items).
    4. :class:`~pgcraft.plugins.api.APIPlugin` -- API view + resource.
    5. :class:`~pgcraft.plugins.simple.SimpleTriggerPlugin` --
       INSTEAD OF triggers.
    6. :class:`~pgcraft.plugins.protect.RawTableProtectionPlugin` --
       BEFORE triggers blocking direct DML on the backing table.

    Pass ``plugins=[...]`` to replace these entirely, or
    ``extra_plugins=[...]`` to append to them.
    """

    DEFAULT_PLUGINS: ClassVar[list[Plugin]] = [
        SerialPKPlugin(),
        SimpleTablePlugin(),
        StatisticsViewPlugin(),
        APIPlugin(),
        SimpleTriggerPlugin(),
        RawTableProtectionPlugin("primary"),
    ]
