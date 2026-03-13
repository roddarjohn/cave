"""Append-only dimension resource factory convenience class."""

from typing import ClassVar

from pgcraft.factory.base import ResourceFactory
from pgcraft.plugin import Plugin
from pgcraft.plugins.api import APIPlugin
from pgcraft.plugins.append_only import (
    AppendOnlyTablePlugin,
    AppendOnlyTriggerPlugin,
    AppendOnlyViewPlugin,
)
from pgcraft.plugins.pk import SerialPKPlugin


class AppendOnlyDimensionResourceFactory(ResourceFactory):
    """Create an append-only (SCD Type 2) dimension.

    Default plugins:

    1. :class:`~pgcraft.plugins.pk.SerialPKPlugin` -- auto-increment PK.
    2. :class:`~pgcraft.plugins.append_only.AppendOnlyTablePlugin` --
       root + attributes tables.
    3. :class:`~pgcraft.plugins.append_only.AppendOnlyViewPlugin` --
       join view.
    4. :class:`~pgcraft.plugins.api.APIPlugin` -- API view + resource.
    5. :class:`~pgcraft.plugins.append_only.AppendOnlyTriggerPlugin` --
       INSTEAD OF triggers.
    """

    DEFAULT_PLUGINS: ClassVar[list[Plugin]] = [
        SerialPKPlugin(),
        AppendOnlyTablePlugin(),
        AppendOnlyViewPlugin(),
        APIPlugin(),
        AppendOnlyTriggerPlugin(),
    ]
