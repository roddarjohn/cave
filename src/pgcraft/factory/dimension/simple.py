"""Simple dimension resource factory."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from pgcraft.factory.base import ResourceFactory
from pgcraft.plugins.protect import RawTableProtectionPlugin
from pgcraft.plugins.simple import (
    _NAMING_DEFAULTS as _SIMPLE_NAMING,
)
from pgcraft.plugins.simple import (
    SimpleTablePlugin,
    _build_simple_ops_with_columns,
)
from pgcraft.plugins.trigger import InsteadOfTriggerPlugin

if TYPE_CHECKING:
    from pgcraft.plugin import Plugin


class PGCraftSimple(ResourceFactory):
    """Create a simple dimension: one table with optional checks.

    Internal plugins (always present):

    1. :class:`~pgcraft.plugins.simple.SimpleTablePlugin` --
       backing table.
    2. :class:`~pgcraft.plugins.trigger.InsteadOfTriggerPlugin`
       -- INSTEAD OF triggers (activates when a view plugin
       produces ``"api"``).

    :class:`~pgcraft.plugins.check.TableCheckPlugin`,
    :class:`~pgcraft.plugins.index.TableIndexPlugin`, and
    :class:`~pgcraft.plugins.fk.TableFKPlugin` are auto-added
    by the base factory when not already present.

    A :class:`~pgcraft.plugins.pk.SerialPKPlugin` is auto-added
    when no user plugin produces ``pk_columns``.

    Pass a
    :class:`~pgcraft.extensions.postgrest.plugin.PostgRESTPlugin`
    via ``extra_plugins`` to expose this table through a
    PostgREST API view with CRUD triggers.

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
        InsteadOfTriggerPlugin(
            ops_builder=_build_simple_ops_with_columns(None, "primary"),
            naming_defaults=_SIMPLE_NAMING,
            function_key="simple_function",
            trigger_key="simple_trigger",
            view_key="api",
            include_private_view=False,
        ),
    ]
