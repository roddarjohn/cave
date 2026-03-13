"""Unit tests for cave.check module."""

import pytest

from cave.check import CaveCheck, collect_checks


class TestCaveCheckColumnNames:
    def test_single_marker(self):
        check = CaveCheck("{price} > 0", name="pos_price")
        assert check.column_names() == ["price"]

    def test_multiple_markers(self):
        check = CaveCheck("{price} * {qty} <= 1000000", name="max_total")
        assert check.column_names() == ["price", "qty"]

    def test_duplicate_markers_deduplicated(self):
        check = CaveCheck("{x} > 0 AND {x} < 100", name="x_range")
        assert check.column_names() == ["x"]

    def test_no_markers(self):
        check = CaveCheck("1 = 1", name="trivial")
        assert check.column_names() == []


class TestCaveCheckResolve:
    def test_identity_mapping(self):
        check = CaveCheck("{price} > 0", name="pos")
        assert check.resolve(lambda c: c) == "price > 0"

    def test_new_prefix_mapping(self):
        check = CaveCheck("{price} > 0", name="pos")
        result = check.resolve(lambda c: f"NEW.{c}")
        assert result == "NEW.price > 0"

    def test_multi_column_resolve(self):
        check = CaveCheck("{price} * {qty} <= 1000000", name="max")
        result = check.resolve(lambda c: f"NEW.{c}")
        assert result == "NEW.price * NEW.qty <= 1000000"

    def test_duplicate_markers_all_resolved(self):
        check = CaveCheck("{x} > 0 AND {x} < 100", name="range")
        result = check.resolve(lambda c: f"NEW.{c}")
        assert result == "NEW.x > 0 AND NEW.x < 100"


class TestCaveCheckFrozen:
    def test_is_immutable(self):
        check = CaveCheck("{a} > 0", name="test")
        with pytest.raises(AttributeError):
            check.expression = "{b} > 0"  # type: ignore[misc]


class TestCollectChecks:
    def test_filters_checks_from_mixed_list(self):
        from sqlalchemy import Column, Integer

        items = [
            Column("price", Integer),
            CaveCheck("{price} > 0", name="pos"),
            Column("qty", Integer),
            CaveCheck("{qty} >= 0", name="nonneg"),
        ]
        result = collect_checks(items)
        assert len(result) == 2
        assert result[0].name == "pos"
        assert result[1].name == "nonneg"

    def test_empty_list(self):
        assert collect_checks([]) == []

    def test_no_checks_in_list(self):
        from sqlalchemy import Column, String

        assert collect_checks([Column("x", String)]) == []
