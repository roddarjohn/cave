"""Statistics view plugin: creates views from PGCraftStatisticsView."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy_declarative_extensions import View, register_view

if TYPE_CHECKING:
    from pgcraft.factory.context import FactoryContext

from pgcraft.plugin import Dynamic, Plugin, produces, requires
from pgcraft.statistics import (
    JoinedView,
    collect_statistics,
)
from pgcraft.utils.query import compile_query


@produces(Dynamic("joins_key"))
@requires(Dynamic("table_key"), "pk_columns")
class StatisticsViewPlugin(Plugin):
    """Create statistics views from PGCraftStatisticsView items.

    For each :class:`~pgcraft.statistics.PGCraftStatisticsView` in
    ``ctx.schema_items``, creates a view (or materialized view)
    and stores a :class:`~pgcraft.statistics.JoinedView` entry in
    ``ctx[joins_key]`` for the API plugin to LEFT JOIN.

    Args:
        joins_key: Key to store the joined view info dict under
            (default ``"joins"``).
        table_key: Key in ``ctx`` for the source table
            (default ``"primary"``).

    """

    def __init__(
        self,
        joins_key: str = "joins",
        table_key: str = "primary",
    ) -> None:
        """Store configuration and context keys."""
        self.joins_key = joins_key
        self.table_key = table_key

    def run(self, ctx: FactoryContext) -> None:
        """Create statistics views and store join info."""
        pk_columns = ctx["pk_columns"]
        pk_col_name = pk_columns.first_key
        stats_items = collect_statistics(ctx.schema_items)
        joins: dict[str, JoinedView] = {}

        for stat in stats_items:
            view_name = f"{ctx.tablename}_{stat.view_suffix}"
            join_key = (
                stat.join_key if stat.join_key is not None else pk_col_name
            )
            view_schema = (
                stat.schema if stat.schema is not None else ctx.schemaname
            )
            view = View(
                view_name,
                compile_query(stat.query),
                schema=view_schema,
                materialized=stat.materialized,
            )
            register_view(ctx.metadata, view)
            exposed_cols = [col for col in stat.column_names if col != join_key]
            joins[stat.name] = JoinedView(
                view_name=f"{view_schema}.{view_name}",
                join_key=join_key,
                column_names=exposed_cols,
            )

        ctx[self.joins_key] = joins
