"""EAV dimension resource factory."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from pgcraft.factory.base import ResourceFactory
from pgcraft.plugins.check import TriggerCheckPlugin
from pgcraft.plugins.created_at import CreatedAtPlugin
from pgcraft.plugins.eav import (
    _NAMING_DEFAULTS as _EAV_NAMING,
)
from pgcraft.plugins.eav import (
    EAVTablePlugin,
    EAVViewPlugin,
    _make_eav_ops_builder,
)
from pgcraft.plugins.protect import RawTableProtectionPlugin
from pgcraft.plugins.trigger import InsteadOfTriggerPlugin

if TYPE_CHECKING:
    from pgcraft.plugin import Plugin


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
    5. :class:`~pgcraft.plugins.trigger.InsteadOfTriggerPlugin`
       -- INSTEAD OF triggers (activates when a view plugin
       produces ``"api"``).

    A :class:`~pgcraft.plugins.pk.SerialPKPlugin` is auto-added
    when no user plugin produces ``pk_columns``.

    Pass a
    :class:`~pgcraft.extensions.postgrest.plugin.PostgRESTPlugin`
    via ``extra_plugins`` to expose this table through a
    PostgREST API view with CRUD triggers.
    """

    _INTERNAL_PLUGINS: ClassVar[list[Plugin]] = [
        CreatedAtPlugin(),
        EAVTablePlugin(),
        EAVViewPlugin(),
        TriggerCheckPlugin(),
        RawTableProtectionPlugin("entity", "attribute"),
        InsteadOfTriggerPlugin(
            ops_builder=_make_eav_ops_builder(
                "entity", "attribute", "eav_mappings"
            ),
            naming_defaults=_EAV_NAMING,
            function_key="eav_function",
            trigger_key="eav_trigger",
            view_key="api",
            extra_requires=[
                "entity",
                "attribute",
                "eav_mappings",
            ],
        ),
    ]
