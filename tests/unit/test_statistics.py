"""Unit tests for pgcraft.statistics module."""

import pytest
from sqlalchemy import Column, Integer, String

from pgcraft.statistics import (
    PGCraftStatistics,
    StatisticsViewInfo,
    collect_statistics,
)


class TestPGCraftStatisticsColumnNames:
    def test_returns_column_names(self):
        stat = PGCraftStatistics(
            name="stats",
            query="SELECT id, COUNT(*) AS cnt FROM t GROUP BY id",
            columns=[("cnt", Integer())],
        )
        assert stat.column_names == ["cnt"]

    def test_multiple_columns(self):
        stat = PGCraftStatistics(
            name="agg",
            query=(
                "SELECT id, SUM(x) AS total, AVG(x) AS avg FROM t GROUP BY id"
            ),
            columns=[("total", Integer()), ("avg", Integer())],
        )
        assert stat.column_names == ["total", "avg"]

    def test_no_columns(self):
        stat = PGCraftStatistics(
            name="empty",
            query="SELECT 1",
        )
        assert stat.column_names == []


class TestPGCraftStatisticsFrozen:
    def test_is_immutable(self):
        stat = PGCraftStatistics(name="s", query="SELECT 1")
        with pytest.raises(AttributeError):
            stat.name = "other"  # type: ignore[misc]


class TestPGCraftStatisticsDefaults:
    def test_materialized_defaults_false(self):
        stat = PGCraftStatistics(name="s", query="SELECT 1")
        assert stat.materialized is False

    def test_join_key_defaults_none(self):
        stat = PGCraftStatistics(name="s", query="SELECT 1")
        assert stat.join_key is None

    def test_columns_defaults_empty(self):
        stat = PGCraftStatistics(name="s", query="SELECT 1")
        assert stat.columns == []


class TestStatisticsViewInfo:
    def test_stores_fields(self):
        info = StatisticsViewInfo(
            view_name="dim.customer_stats",
            join_key="id",
            column_names=["cnt"],
        )
        assert info.view_name == "dim.customer_stats"
        assert info.join_key == "id"
        assert info.column_names == ["cnt"]


class TestCollectStatistics:
    def test_filters_statistics_from_mixed_list(self):
        items = [
            Column("name", String),
            PGCraftStatistics(
                name="stats",
                query="SELECT 1",
                columns=[("cnt", Integer())],
            ),
            Column("age", Integer),
        ]
        result = collect_statistics(items)
        assert len(result) == 1
        assert result[0].name == "stats"

    def test_empty_list(self):
        assert collect_statistics([]) == []

    def test_no_statistics_in_list(self):
        assert collect_statistics([Column("x", String)]) == []
