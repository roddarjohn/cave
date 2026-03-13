"""Unit tests for StatisticsViewPlugin in isolation."""

from sqlalchemy import Column, Integer, MetaData, String, Table

from pgcraft.plugins.statistics import StatisticsViewPlugin
from pgcraft.statistics import PGCraftStatistics
from tests.unit.plugins.conftest import make_ctx


def _ctx_with_stats(stats_items=None):
    """Return a ctx with a Table and PGCraftStatistics items."""
    schema_items = list(stats_items or [])
    schema_items.append(Column("name", String))
    ctx = make_ctx(schemaname="dim", schema_items=schema_items)
    table = Table(
        "product",
        MetaData(),
        Column("id", Integer, primary_key=True),
        Column("name", String),
        schema="dim",
    )
    ctx["primary"] = table
    return ctx


class TestStatisticsViewPlugin:
    def test_creates_view_in_metadata(self):
        stat = PGCraftStatistics(
            name="statistics",
            query="SELECT id, COUNT(*) AS cnt FROM orders GROUP BY id",
            columns=[("cnt", Integer())],
        )
        plugin = StatisticsViewPlugin()
        ctx = _ctx_with_stats([stat])
        plugin.run(ctx)
        views = ctx.metadata.info.get("views")
        assert views is not None
        assert len(views.views) == 1

    def test_view_name_includes_suffix(self):
        stat = PGCraftStatistics(
            name="statistics",
            query="SELECT 1",
            columns=[("cnt", Integer())],
        )
        plugin = StatisticsViewPlugin()
        ctx = _ctx_with_stats([stat])
        plugin.run(ctx)
        view = ctx.metadata.info["views"].views[0]
        assert view.name == "product_statistics"

    def test_view_schema_matches_ctx(self):
        stat = PGCraftStatistics(
            name="stats",
            query="SELECT 1",
        )
        plugin = StatisticsViewPlugin()
        ctx = _ctx_with_stats([stat])
        plugin.run(ctx)
        view = ctx.metadata.info["views"].views[0]
        assert view.schema == "dim"

    def test_stores_info_in_ctx(self):
        stat = PGCraftStatistics(
            name="statistics",
            query="SELECT id, COUNT(*) AS cnt FROM t GROUP BY id",
            columns=[("cnt", Integer())],
        )
        plugin = StatisticsViewPlugin()
        ctx = _ctx_with_stats([stat])
        plugin.run(ctx)
        info = ctx["statistics_views"]
        assert "statistics" in info
        assert info["statistics"].view_name == "dim.product_statistics"
        assert info["statistics"].join_key == "id"
        assert info["statistics"].column_names == ["cnt"]

    def test_custom_join_key(self):
        stat = PGCraftStatistics(
            name="stats",
            query=(
                "SELECT customer_id, COUNT(*) AS cnt"
                " FROM t GROUP BY customer_id"
            ),
            columns=[("cnt", Integer())],
            join_key="customer_id",
        )
        plugin = StatisticsViewPlugin()
        ctx = _ctx_with_stats([stat])
        plugin.run(ctx)
        info = ctx["statistics_views"]
        assert info["stats"].join_key == "customer_id"

    def test_custom_stats_key(self):
        stat = PGCraftStatistics(
            name="stats",
            query="SELECT 1",
        )
        plugin = StatisticsViewPlugin(stats_key="my_stats")
        ctx = _ctx_with_stats([stat])
        plugin.run(ctx)
        assert "my_stats" in ctx

    def test_no_stats_items_produces_empty_dict(self):
        plugin = StatisticsViewPlugin()
        ctx = _ctx_with_stats([])
        plugin.run(ctx)
        assert ctx["statistics_views"] == {}

    def test_materialized_view(self):
        stat = PGCraftStatistics(
            name="summary",
            query="SELECT 1",
            materialized=True,
        )
        plugin = StatisticsViewPlugin()
        ctx = _ctx_with_stats([stat])
        plugin.run(ctx)
        view = ctx.metadata.info["views"].views[0]
        assert view.materialized is True

    def test_multiple_stats_views(self):
        stats = [
            PGCraftStatistics(
                name="counts",
                query="SELECT id, COUNT(*) AS cnt FROM t GROUP BY id",
                columns=[("cnt", Integer())],
            ),
            PGCraftStatistics(
                name="totals",
                query="SELECT id, SUM(x) AS total FROM t GROUP BY id",
                columns=[("total", Integer())],
            ),
        ]
        plugin = StatisticsViewPlugin()
        ctx = _ctx_with_stats(stats)
        plugin.run(ctx)
        views = ctx.metadata.info["views"]
        assert len(views.views) == 2
        info = ctx["statistics_views"]
        assert "counts" in info
        assert "totals" in info
