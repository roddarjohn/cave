"""EAV dimension factory convenience class."""

from typing import ClassVar

from cave.factory.base import DimensionFactory
from cave.plugin import Plugin
from cave.plugins.api import APIPlugin
from cave.plugins.eav import EAVTablePlugin, EAVTriggerPlugin, EAVViewPlugin
from cave.plugins.pk import SerialPKPlugin


class EAVDimensionFactory(DimensionFactory):
    """Create an EAV (Entity-Attribute-Value) dimension.

    Default plugins:

    1. :class:`~cave.plugins.pk.SerialPKPlugin` -- auto-increment PK.
    2. :class:`~cave.plugins.eav.EAVTablePlugin` -- entity + attribute tables.
    3. :class:`~cave.plugins.eav.EAVViewPlugin` -- pivot view.
    4. :class:`~cave.plugins.api.APIPlugin` -- API view + resource.
    5. :class:`~cave.plugins.eav.EAVTriggerPlugin` -- INSTEAD OF triggers.
    """

    DEFAULT_PLUGINS: ClassVar[list[Plugin]] = [
        SerialPKPlugin(),
        EAVTablePlugin(),
        EAVViewPlugin(),
        APIPlugin(),
        EAVTriggerPlugin(),
    ]
