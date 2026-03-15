"""Append-only dimension resource factory."""

from typing import ClassVar

from pgcraft.factory.base import ResourceFactory
from pgcraft.plugin import Plugin
from pgcraft.plugins.append_only import (
    AppendOnlyTablePlugin,
    AppendOnlyTriggerPlugin,
    AppendOnlyViewPlugin,
)
from pgcraft.plugins.check import TableCheckPlugin
from pgcraft.plugins.created_at import CreatedAtPlugin
from pgcraft.plugins.protect import RawTableProtectionPlugin


class PGCraftAppendOnly(ResourceFactory):
    """Create an append-only (SCD Type 2) dimension.

    Internal plugins (always present):

    1. :class:`~pgcraft.plugins.created_at.CreatedAtPlugin` --
       ``created_at`` column name.
    2. :class:`~pgcraft.plugins.append_only.AppendOnlyTablePlugin`
       -- root + attributes tables.
    3. :class:`~pgcraft.plugins.append_only.AppendOnlyViewPlugin`
       -- join view proxy.
    4. :class:`~pgcraft.plugins.check.TableCheckPlugin` --
       materializes :class:`~pgcraft.check.PGCraftCheck` items.

    A :class:`~pgcraft.plugins.pk.SerialPKPlugin` is auto-added
    when no user plugin produces ``pk_columns``.

    Use :class:`~pgcraft.views.api.APIView` to expose this table
    through a PostgREST API view with CRUD triggers.
    """

    _INTERNAL_PLUGINS: ClassVar[list[Plugin]] = [
        CreatedAtPlugin(),
        AppendOnlyTablePlugin(),
        AppendOnlyViewPlugin(),
        TableCheckPlugin(),
        RawTableProtectionPlugin("root_table", "attributes"),
    ]

    TRIGGER_PLUGIN_CLS = AppendOnlyTriggerPlugin
