"""Simple dimension resource factory."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from pgcraft.factory.base import ResourceFactory
from pgcraft.plugins.protect import RawTableProtectionPlugin
from pgcraft.plugins.simple import SimpleTablePlugin

if TYPE_CHECKING:
    from pgcraft.plugin import Plugin


class PGCraftSimple(ResourceFactory):
    """Create a simple dimension: one table with optional checks.

    Internal plugins (always present):

    1. :class:`~pgcraft.plugins.simple.SimpleTablePlugin` --
       backing table.

    :class:`~pgcraft.plugins.check.TableCheckPlugin`,
    :class:`~pgcraft.plugins.index.TableIndexPlugin`, and
    :class:`~pgcraft.plugins.fk.TableFKPlugin` are auto-added
    by the base factory when not already present.

    A :class:`~pgcraft.plugins.pk.SerialPKPlugin` is auto-added
    when no user plugin produces ``pk_columns``.

    Use :class:`~pgcraft.extensions.postgrest.PostgRESTView`
    to expose this table through a PostgREST API view with
    CRUD triggers.

    Args:
        tablename: Name of the dimension table.
        schemaname: PostgreSQL schema for generated objects.
        metadata: SQLAlchemy ``MetaData`` to register on.
        schema_items: Column and constraint definitions.
        plugins: Behaviour-modifying plugins (e.g.
            ``UUIDV4PKPlugin``).
        extra_plugins: Appended to the resolved plugin list.

    """

    _INTERNAL_PLUGINS: ClassVar[list[Plugin]] = [
        SimpleTablePlugin(),
        RawTableProtectionPlugin("primary"),
    ]
