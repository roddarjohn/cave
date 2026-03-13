"""Statistics view plugin: creates views from PGCraftStatistics."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy_declarative_extensions import View, register_view

if TYPE_CHECKING:
    from pgcraft.factory.context import FactoryContext

from pgcraft.plugin import Dynamic, Plugin, produces, requires
from pgcraft.statistics import (
    StatisticsViewInfo,
    collect_statistics,
)


@produces(Dynamic("stats_key"))
@requires(Dynamic("table_key"), "pk_columns")
class StatisticsViewPlugin(Plugin):
    """Create statistics views from PGCraftStatistics items.

    For each :class:`~pgcraft.statistics.PGCraftStatistics` in
    ``ctx.schema_items``, creates a view (or materialized view)
    and stores info in ``ctx[stats_key]`` for the API plugin.

    Args:
        stats_key: Key to store view info dict under
            (default ``"statistics_views"``).
        table_key: Key in ``ctx`` for the source table
            (default ``"primary"``).

    """

    def __init__(
        self,
        stats_key: str = "statistics_views",
        table_key: str = "primary",
    ) -> None:
        """Store configuration and context keys."""
        self.stats_key = stats_key
        self.table_key = table_key

    def run(self, ctx: FactoryContext) -> None:
        """Create statistics views and store info for API."""
        pk_columns = ctx["pk_columns"]
        pk_col_name = pk_columns.first_key
        stats_items = collect_statistics(ctx.schema_items)
        info: dict[str, StatisticsViewInfo] = {}

        for stat in stats_items:
            view_name = f"{ctx.tablename}_{stat.name}"
            join_key = (
                stat.join_key if stat.join_key is not None else pk_col_name
            )
            view = View(
                view_name,
                stat.query,
                schema=ctx.schemaname,
                materialized=stat.materialized,
            )
            register_view(ctx.metadata, view)
            info[stat.name] = StatisticsViewInfo(
                view_name=(f"{ctx.schemaname}.{view_name}"),
                join_key=join_key,
                column_names=stat.column_names,
            )

        ctx[self.stats_key] = info
