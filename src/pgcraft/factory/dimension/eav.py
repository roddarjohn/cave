"""EAV dimension resource factory convenience class."""

from typing import ClassVar

from pgcraft.factory.base import ResourceFactory
from pgcraft.plugin import Plugin
from pgcraft.plugins.api import APIPlugin
from pgcraft.plugins.eav import EAVTablePlugin, EAVTriggerPlugin, EAVViewPlugin
from pgcraft.plugins.pk import SerialPKPlugin


class EAVDimensionResourceFactory(ResourceFactory):
    """Create an EAV (Entity-Attribute-Value) dimension.

    Default plugins:

    1. :class:`~pgcraft.plugins.pk.SerialPKPlugin` -- auto-increment PK.
    2. :class:`~pgcraft.plugins.eav.EAVTablePlugin` -- entity +
       attribute tables.
    3. :class:`~pgcraft.plugins.eav.EAVViewPlugin` -- pivot view.
    4. :class:`~pgcraft.plugins.api.APIPlugin` -- API view + resource.
    5. :class:`~pgcraft.plugins.eav.EAVTriggerPlugin` -- INSTEAD OF triggers.
    """

    DEFAULT_PLUGINS: ClassVar[list[Plugin]] = [
        SerialPKPlugin(),
        EAVTablePlugin(),
        EAVViewPlugin(),
        APIPlugin(),
        EAVTriggerPlugin(),
    ]
