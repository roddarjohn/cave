"""Append-only dimension resource factory."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from pgcraft.factory.base import ResourceFactory
from pgcraft.plugins.append_only import (
    _NAMING_DEFAULTS as _AO_NAMING,
)
from pgcraft.plugins.append_only import (
    AppendOnlyTablePlugin,
    AppendOnlyViewPlugin,
)
from pgcraft.plugins.append_only import (
    _make_ops_builder as _ao_ops_builder,
)
from pgcraft.plugins.created_at import CreatedAtPlugin
from pgcraft.plugins.fk import TableFKPlugin
from pgcraft.plugins.index import TableIndexPlugin
from pgcraft.plugins.protect import RawTableProtectionPlugin
from pgcraft.plugins.trigger import InsteadOfTriggerPlugin

if TYPE_CHECKING:
    from pgcraft.plugin import Plugin


class PGCraftAppendOnly(ResourceFactory):
    """Create an append-only (SCD Type 2) dimension.

    Internal plugins (always present):

    1. :class:`~pgcraft.plugins.created_at.CreatedAtPlugin` --
       ``created_at`` column name.
    2. :class:`~pgcraft.plugins.append_only.AppendOnlyTablePlugin`
       -- root + attributes tables.
    3. :class:`~pgcraft.plugins.append_only.AppendOnlyViewPlugin`
       -- join view proxy.
    4. :class:`~pgcraft.plugins.index.TableIndexPlugin` --
       indices on the attributes table.
    5. :class:`~pgcraft.plugins.fk.TableFKPlugin` --
       foreign keys on the attributes table.
    6. :class:`~pgcraft.plugins.trigger.InsteadOfTriggerPlugin`
       -- INSTEAD OF triggers (activates when a view plugin
       produces ``"api"``).

    :class:`~pgcraft.plugins.check.TableCheckPlugin` is
    auto-added by the base factory when not already present.

    A :class:`~pgcraft.plugins.pk.SerialPKPlugin` is auto-added
    when no user plugin produces ``pk_columns``.

    Pass a
    :class:`~pgcraft.extensions.postgrest.plugin.PostgRESTPlugin`
    via ``extra_plugins`` to expose this table through a
    PostgREST API view with CRUD triggers.
    """

    _FK_TARGET_KEY: ClassVar[str] = "root_table"

    _INTERNAL_PLUGINS: ClassVar[list[Plugin]] = [
        CreatedAtPlugin(),
        AppendOnlyTablePlugin(),
        AppendOnlyViewPlugin(),
        TableIndexPlugin(table_key="attributes"),
        TableFKPlugin(table_key="attributes"),
        RawTableProtectionPlugin("root_table", "attributes"),
        InsteadOfTriggerPlugin(
            ops_builder=_ao_ops_builder("root_table", "attributes"),
            naming_defaults=_AO_NAMING,
            function_key="append_only_function",
            trigger_key="append_only_trigger",
            view_key="api",
            extra_requires=[
                "root_table",
                "attributes",
            ],
        ),
    ]
