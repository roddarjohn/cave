"""Unit tests for pgcraft.statistics module."""

import pytest
from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    String,
    Table,
    func,
    select,
)

from pgcraft.statistics import (
    PGCraftStatisticsView,
    StatisticsViewInfo,
    collect_statistics,
)

_md = MetaData()
_orders = Table(
    "orders",
    _md,
    Column("id", Integer, primary_key=True),
    Column("customer_id", Integer),
    Column("total", Integer),
)


class TestPGCraftStatisticsViewColumnNames:
    def test_returns_column_names(self):
        stat = PGCraftStatisticsView(
            name="orders",
            query=select(
                _orders.c.customer_id,
                func.count().label("cnt"),
            ).group_by(_orders.c.customer_id),
        )
        assert stat.column_names == ["customer_id", "cnt"]

    def test_multiple_columns(self):
        stat = PGCraftStatisticsView(
            name="orders",
            query=select(
                _orders.c.customer_id,
                func.sum(_orders.c.total).label("total"),
                func.avg(_orders.c.total).label("avg"),
            ).group_by(_orders.c.customer_id),
        )
        assert stat.column_names == [
            "customer_id",
            "total",
            "avg",
        ]


class TestPGCraftStatisticsViewSuffix:
    def test_appends_statistics(self):
        stat = PGCraftStatisticsView(
            name="orders",
            query=select(_orders.c.id),
        )
        assert stat.view_suffix == "orders_statistics"

    def test_different_name(self):
        stat = PGCraftStatisticsView(
            name="invoices",
            query=select(_orders.c.id),
        )
        assert stat.view_suffix == "invoices_statistics"


class TestPGCraftStatisticsViewFrozen:
    def test_is_immutable(self):
        stat = PGCraftStatisticsView(
            name="s",
            query=select(_orders.c.id),
        )
        with pytest.raises(AttributeError):
            stat.name = "other"  # type: ignore[misc]


class TestPGCraftStatisticsViewDefaults:
    def test_materialized_defaults_false(self):
        stat = PGCraftStatisticsView(
            name="s",
            query=select(_orders.c.id),
        )
        assert stat.materialized is False

    def test_join_key_defaults_none(self):
        stat = PGCraftStatisticsView(
            name="s",
            query=select(_orders.c.id),
        )
        assert stat.join_key is None

    def test_schema_defaults_none(self):
        stat = PGCraftStatisticsView(
            name="s",
            query=select(_orders.c.id),
        )
        assert stat.schema is None

    def test_custom_schema(self):
        stat = PGCraftStatisticsView(
            name="s",
            query=select(_orders.c.id),
            schema="analytics",
        )
        assert stat.schema == "analytics"


class TestStatisticsViewInfo:
    def test_stores_fields(self):
        info = StatisticsViewInfo(
            view_name="dim.customer_orders_statistics",
            join_key="customer_id",
            column_names=["cnt"],
        )
        assert info.view_name == "dim.customer_orders_statistics"
        assert info.join_key == "customer_id"
        assert info.column_names == ["cnt"]


class TestCollectStatistics:
    def test_filters_statistics_from_mixed_list(self):
        items = [
            Column("name", String),
            PGCraftStatisticsView(
                name="orders",
                query=select(
                    _orders.c.customer_id,
                    func.count().label("cnt"),
                ).group_by(_orders.c.customer_id),
            ),
            Column("age", Integer),
        ]
        result = collect_statistics(items)
        assert len(result) == 1
        assert result[0].name == "orders"

    def test_empty_list(self):
        assert collect_statistics([]) == []

    def test_no_statistics_in_list(self):
        assert collect_statistics([Column("x", String)]) == []
