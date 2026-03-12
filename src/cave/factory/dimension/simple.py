"""Simple dimension factory convenience class."""

from typing import ClassVar

from cave.factory.base import DimensionFactory
from cave.plugin import Plugin
from cave.plugins.api import APIPlugin
from cave.plugins.pk import SerialPKPlugin
from cave.plugins.simple import SimpleTablePlugin, SimpleTriggerPlugin


class SimpleDimensionFactory(DimensionFactory):
    """Create a simple dimension: one table, one API view, CRUD triggers.

    Default plugins:

    1. :class:`~cave.plugins.pk.SerialPKPlugin` -- auto-increment PK.
    2. :class:`~cave.plugins.simple.SimpleTablePlugin` -- backing table.
    3. :class:`~cave.plugins.api.APIPlugin` -- API view + resource.
    4. :class:`~cave.plugins.simple.SimpleTriggerPlugin` -- INSTEAD OF triggers.

    Pass ``plugins=[...]`` to replace these entirely, or
    ``extra_plugins=[...]`` to append to them.
    """

    DEFAULT_PLUGINS: ClassVar[list[Plugin]] = [
        SerialPKPlugin(),
        SimpleTablePlugin(),
        APIPlugin(),
        SimpleTriggerPlugin(),
    ]
