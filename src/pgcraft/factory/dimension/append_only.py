"""Append-only dimension resource factory."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from pgcraft.factory.base import ResourceFactory
from pgcraft.plugins.append_only import (
    AppendOnlyTablePlugin,
    AppendOnlyViewPlugin,
)
from pgcraft.plugins.created_at import CreatedAtPlugin
from pgcraft.plugins.fk import TableFKPlugin
from pgcraft.plugins.index import TableIndexPlugin
from pgcraft.plugins.protect import RawTableProtectionPlugin

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

    :class:`~pgcraft.plugins.check.TableCheckPlugin` is
    auto-added by the base factory when not already present.

    A :class:`~pgcraft.plugins.pk.SerialPKPlugin` is auto-added
    when no user plugin produces ``pk_columns``.

    Use :class:`~pgcraft.extensions.postgrest.PostgRESTView`
    to expose this table through a PostgREST API view with
    CRUD triggers.
    """

    _FK_TARGET_KEY: ClassVar[str] = "root_table"

    _INTERNAL_PLUGINS: ClassVar[list[Plugin]] = [
        CreatedAtPlugin(),
        AppendOnlyTablePlugin(),
        AppendOnlyViewPlugin(),
        TableIndexPlugin(table_key="attributes"),
        TableFKPlugin(table_key="attributes"),
        RawTableProtectionPlugin("root_table", "attributes"),
    ]
