"""EAV dimension resource factory."""

from typing import ClassVar

from pgcraft.factory.base import ResourceFactory
from pgcraft.plugin import Plugin
from pgcraft.plugins.check import TriggerCheckPlugin
from pgcraft.plugins.created_at import CreatedAtPlugin
from pgcraft.plugins.eav import (
    EAVTablePlugin,
    EAVTriggerPlugin,
    EAVViewPlugin,
)
from pgcraft.plugins.protect import RawTableProtectionPlugin


class PGCraftEAV(ResourceFactory):
    """Create an EAV (Entity-Attribute-Value) dimension.

    Internal plugins (always present):

    1. :class:`~pgcraft.plugins.created_at.CreatedAtPlugin` --
       ``created_at`` column name.
    2. :class:`~pgcraft.plugins.eav.EAVTablePlugin` -- entity +
       attribute tables.
    3. :class:`~pgcraft.plugins.eav.EAVViewPlugin` -- pivot view
       proxy.
    4. :class:`~pgcraft.plugins.check.TriggerCheckPlugin` --
       trigger-based checks on the pivot view.

    A :class:`~pgcraft.plugins.pk.SerialPKPlugin` is auto-added
    when no user plugin produces ``pk_columns``.

    Use :class:`~pgcraft.extensions.postgrest.PostgRESTView`
    to expose this table through a PostgREST API view with
    CRUD triggers.
    """

    _INTERNAL_PLUGINS: ClassVar[list[Plugin]] = [
        CreatedAtPlugin(),
        EAVTablePlugin(),
        EAVViewPlugin(),
        TriggerCheckPlugin(),
        RawTableProtectionPlugin("entity", "attribute"),
    ]

    TRIGGER_PLUGIN_CLS = staticmethod(EAVTriggerPlugin)
