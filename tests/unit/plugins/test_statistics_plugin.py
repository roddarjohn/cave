"""Unit tests for StatisticsViewPlugin in isolation."""

from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    String,
    Table,
    func,
    select,
)

from pgcraft.plugins.statistics import StatisticsViewPlugin
from pgcraft.statistics import PGCraftStatisticsView
from tests.unit.plugins.conftest import make_ctx

_md = MetaData()
_orders = Table(
    "orders",
    _md,
    Column("id", Integer, primary_key=True),
    Column("customer_id", Integer),
    Column("total", Integer),
)
_invoices = Table(
    "invoices",
    _md,
    Column("id", Integer, primary_key=True),
    Column("customer_id", Integer),
    Column("amount", Integer),
)


def _ctx_with_stats(stats_items=None):
    """Return a ctx with a Table and PGCraftStatisticsView items."""
    schema_items = list(stats_items or [])
    schema_items.append(Column("name", String))
    ctx = make_ctx(
        tablename="customer",
        schemaname="dim",
        schema_items=schema_items,
    )
    table = Table(
        "customer",
        MetaData(),
        Column("id", Integer, primary_key=True),
        Column("name", String),
        schema="dim",
    )
    ctx["primary"] = table
    return ctx


def _order_stats_query():
    return select(
        _orders.c.customer_id,
        func.count().label("order_count"),
    ).group_by(_orders.c.customer_id)


def _invoice_stats_query():
    return select(
        _invoices.c.customer_id,
        func.sum(_invoices.c.amount).label("total_invoiced"),
    ).group_by(_invoices.c.customer_id)


class TestStatisticsViewPlugin:
    def test_creates_view_in_metadata(self):
        stat = PGCraftStatisticsView(
            name="orders",
            query=_order_stats_query(),
            join_key="customer_id",
        )
        plugin = StatisticsViewPlugin()
        ctx = _ctx_with_stats([stat])
        plugin.run(ctx)
        views = ctx.metadata.info.get("views")
        assert views is not None
        assert len(views.views) == 1

    def test_view_name_follows_convention(self):
        stat = PGCraftStatisticsView(
            name="orders",
            query=_order_stats_query(),
            join_key="customer_id",
        )
        plugin = StatisticsViewPlugin()
        ctx = _ctx_with_stats([stat])
        plugin.run(ctx)
        view = ctx.metadata.info["views"].views[0]
        assert view.name == "customer_orders_statistics"

    def test_view_schema_matches_ctx(self):
        stat = PGCraftStatisticsView(
            name="orders",
            query=_order_stats_query(),
            join_key="customer_id",
        )
        plugin = StatisticsViewPlugin()
        ctx = _ctx_with_stats([stat])
        plugin.run(ctx)
        view = ctx.metadata.info["views"].views[0]
        assert view.schema == "dim"

    def test_stores_info_in_ctx(self):
        stat = PGCraftStatisticsView(
            name="orders",
            query=_order_stats_query(),
            join_key="customer_id",
        )
        plugin = StatisticsViewPlugin()
        ctx = _ctx_with_stats([stat])
        plugin.run(ctx)
        info = ctx["statistics_views"]
        assert "orders" in info
        si = info["orders"]
        assert si.view_name == "dim.customer_orders_statistics"
        assert si.join_key == "customer_id"
        assert si.column_names == ["order_count"]

    def test_join_key_excluded_from_column_names(self):
        stat = PGCraftStatisticsView(
            name="orders",
            query=_order_stats_query(),
            join_key="customer_id",
        )
        plugin = StatisticsViewPlugin()
        ctx = _ctx_with_stats([stat])
        plugin.run(ctx)
        info = ctx["statistics_views"]
        assert "customer_id" not in info["orders"].column_names

    def test_join_key_defaults_to_pk(self):
        stat = PGCraftStatisticsView(
            name="orders",
            query=select(
                _orders.c.id,
                func.count().label("cnt"),
            ).group_by(_orders.c.id),
        )
        plugin = StatisticsViewPlugin()
        ctx = _ctx_with_stats([stat])
        plugin.run(ctx)
        info = ctx["statistics_views"]
        assert info["orders"].join_key == "id"
        assert info["orders"].column_names == ["cnt"]

    def test_custom_stats_key(self):
        stat = PGCraftStatisticsView(
            name="orders",
            query=_order_stats_query(),
            join_key="customer_id",
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
        stat = PGCraftStatisticsView(
            name="orders",
            query=_order_stats_query(),
            join_key="customer_id",
            materialized=True,
        )
        plugin = StatisticsViewPlugin()
        ctx = _ctx_with_stats([stat])
        plugin.run(ctx)
        view = ctx.metadata.info["views"].views[0]
        assert view.materialized is True

    def test_custom_schema(self):
        stat = PGCraftStatisticsView(
            name="orders",
            query=_order_stats_query(),
            join_key="customer_id",
            schema="analytics",
        )
        plugin = StatisticsViewPlugin()
        ctx = _ctx_with_stats([stat])
        plugin.run(ctx)
        view = ctx.metadata.info["views"].views[0]
        assert view.schema == "analytics"
        info = ctx["statistics_views"]
        assert info["orders"].view_name == (
            "analytics.customer_orders_statistics"
        )

    def test_multiple_stats_views(self):
        stats = [
            PGCraftStatisticsView(
                name="orders",
                query=_order_stats_query(),
                join_key="customer_id",
            ),
            PGCraftStatisticsView(
                name="invoices",
                query=_invoice_stats_query(),
                join_key="customer_id",
            ),
        ]
        plugin = StatisticsViewPlugin()
        ctx = _ctx_with_stats(stats)
        plugin.run(ctx)
        views = ctx.metadata.info["views"]
        assert len(views.views) == 2
        info = ctx["statistics_views"]
        assert "orders" in info
        assert "invoices" in info
        assert info["orders"].column_names == ["order_count"]
        assert info["invoices"].column_names == ["total_invoiced"]
        view_names = {v.name for v in views.views}
        assert "customer_orders_statistics" in view_names
        assert "customer_invoices_statistics" in view_names

    def test_view_definition_contains_compiled_sql(self):
        stat = PGCraftStatisticsView(
            name="orders",
            query=_order_stats_query(),
            join_key="customer_id",
        )
        plugin = StatisticsViewPlugin()
        ctx = _ctx_with_stats([stat])
        plugin.run(ctx)
        view = ctx.metadata.info["views"].views[0]
        assert "customer_id" in view.definition
        assert "count" in view.definition.lower()
