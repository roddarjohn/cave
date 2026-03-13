"""Simple dimension resource factory convenience class."""

from typing import ClassVar

from pgcraft.factory.base import ResourceFactory
from pgcraft.plugin import Plugin
from pgcraft.plugins.api import APIPlugin
from pgcraft.plugins.pk import SerialPKPlugin
from pgcraft.plugins.simple import SimpleTablePlugin, SimpleTriggerPlugin


class SimpleDimensionResourceFactory(ResourceFactory):
    """Create a simple dimension: one table, one API view, CRUD triggers.

    Default plugins:

    1. :class:`~pgcraft.plugins.pk.SerialPKPlugin` -- auto-increment PK.
    2. :class:`~pgcraft.plugins.simple.SimpleTablePlugin` -- backing table.
    3. :class:`~pgcraft.plugins.api.APIPlugin` -- API view + resource.
    4. :class:`~pgcraft.plugins.simple.SimpleTriggerPlugin` --
       INSTEAD OF triggers.

    Pass ``plugins=[...]`` to replace these entirely, or
    ``extra_plugins=[...]`` to append to them.
    """

    DEFAULT_PLUGINS: ClassVar[list[Plugin]] = [
        SerialPKPlugin(),
        SimpleTablePlugin(),
        APIPlugin(),
        SimpleTriggerPlugin(),
    ]
