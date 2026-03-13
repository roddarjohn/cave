"""EAV dimension resource factory convenience class."""

from typing import ClassVar

from pgcraft.factory.base import ResourceFactory
from pgcraft.plugin import Plugin
from pgcraft.plugins.api import APIPlugin
from pgcraft.plugins.created_at import CreatedAtPlugin
from pgcraft.plugins.eav import EAVTablePlugin, EAVTriggerPlugin, EAVViewPlugin
from pgcraft.plugins.pk import SerialPKPlugin
from pgcraft.plugins.statistics import StatisticsViewPlugin


class EAVDimensionResourceFactory(ResourceFactory):
    """Create an EAV (Entity-Attribute-Value) dimension.

    Default plugins:

    1. :class:`~pgcraft.plugins.pk.SerialPKPlugin` -- auto-increment PK.
    2. :class:`~pgcraft.plugins.created_at.CreatedAtPlugin` --
       ``created_at`` timestamp on entity table.
    3. :class:`~pgcraft.plugins.eav.EAVTablePlugin` -- entity +
       attribute tables.
    4. :class:`~pgcraft.plugins.eav.EAVViewPlugin` -- pivot view.
    5. :class:`~pgcraft.plugins.statistics.StatisticsViewPlugin` --
       statistics views (no-op when no statistics items).
    6. :class:`~pgcraft.plugins.api.APIPlugin` -- API view + resource.
    7. :class:`~pgcraft.plugins.eav.EAVTriggerPlugin` -- INSTEAD OF triggers.
    """

    DEFAULT_PLUGINS: ClassVar[list[Plugin]] = [
        SerialPKPlugin(),
        CreatedAtPlugin(),
        EAVTablePlugin(),
        EAVViewPlugin(),
        StatisticsViewPlugin(),
        APIPlugin(),
        EAVTriggerPlugin(),
    ]
