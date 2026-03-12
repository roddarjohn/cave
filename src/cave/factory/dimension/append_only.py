"""Append-only dimension factory convenience class."""

from typing import ClassVar

from cave.factory.base import DimensionFactory
from cave.plugin import Plugin
from cave.plugins.api import APIPlugin
from cave.plugins.append_only import (
    AppendOnlyTablePlugin,
    AppendOnlyTriggerPlugin,
    AppendOnlyViewPlugin,
)
from cave.plugins.pk import SerialPKPlugin


class AppendOnlyDimensionFactory(DimensionFactory):
    """Create an append-only (SCD Type 2) dimension.

    Default plugins:

    1. :class:`~cave.plugins.pk.SerialPKPlugin` -- auto-increment PK.
    2. :class:`~cave.plugins.append_only.AppendOnlyTablePlugin` --
       root + attributes tables.
    3. :class:`~cave.plugins.append_only.AppendOnlyViewPlugin` --
       join view.
    4. :class:`~cave.plugins.api.APIPlugin` -- API view + resource.
    5. :class:`~cave.plugins.append_only.AppendOnlyTriggerPlugin` --
       INSTEAD OF triggers.
    """

    DEFAULT_PLUGINS: ClassVar[list[Plugin]] = [
        SerialPKPlugin(),
        AppendOnlyTablePlugin(),
        AppendOnlyViewPlugin(),
        APIPlugin(),
        AppendOnlyTriggerPlugin(),
    ]
