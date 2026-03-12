"""Plugin base class for cave factory extensions."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy import Column

    from cave.factory.context import FactoryContext


class Plugin:
    """Base class for cave factory plugins.

    Each plugin can implement any subset of the lifecycle hooks.
    All methods are no-ops by default.

    Lifecycle order per factory invocation:

    1. ``pk_columns`` -- first non-None result across all plugins
       is used as the PK column list.
    2. ``extra_columns`` -- all results are concatenated and
       prepended to the dimension list in ``ctx.extra_columns``.
    3. ``create_tables`` -- all plugins called in order.
    4. ``create_views`` -- all plugins called in order.
    5. ``create_triggers`` -- all plugins called in order.
    6. ``post_create`` -- all plugins called in order.

    Convention: the plugin responsible for the primary storage must
    set ``ctx.tables["primary"]`` so that ``APIPlugin`` can build the
    API view from it.  For patterns where the "primary queryable" is
    a view (append-only, EAV), set ``ctx.tables["primary"]`` in
    ``create_views`` instead -- this still works because
    ``APIPlugin.create_views`` comes after the storage view plugin
    in the plugin list.
    """

    def pk_columns(self, _ctx: FactoryContext) -> list[Column] | None:
        """Return PK column(s) for the root table.

        The first non-None result across all plugins is used.
        Return ``None`` to defer to the next plugin.

        Args:
            _ctx: The factory context.

        Returns:
            A list of primary key columns, or ``None`` to skip.

        """
        return None

    def extra_columns(self, _ctx: FactoryContext) -> list[Column]:
        """Return additional columns to include before the user dimensions.

        Results from all plugins are concatenated and stored in
        ``ctx.extra_columns`` before ``create_tables`` is called.

        Args:
            _ctx: The factory context.

        Returns:
            A (possibly empty) list of extra columns.

        """
        return []

    def create_tables(self, ctx: FactoryContext) -> None:
        """Create backing tables for this dimension.

        For simple storage patterns, set ``ctx.tables["primary"]``
        to the created table here.

        Args:
            ctx: The factory context with resolved pk/extra columns.

        """

    def create_views(self, ctx: FactoryContext) -> None:
        """Create views on top of the backing tables.

        For append-only and EAV patterns, set
        ``ctx.tables["primary"]`` to the view proxy here, before
        ``APIPlugin.create_views`` runs.

        Args:
            ctx: The factory context after table creation.

        """

    def create_triggers(self, ctx: FactoryContext) -> None:
        """Create INSTEAD OF trigger functions and register them.

        Args:
            ctx: The factory context after view creation.

        """

    def post_create(self, ctx: FactoryContext) -> None:
        """Run the final lifecycle hook.

        Used for API resource registration, custom metadata, etc.

        Args:
            ctx: The fully populated factory context.

        """
