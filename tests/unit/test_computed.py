"""Unit tests for pgcraft.computed module."""

import pytest
from sqlalchemy import Column, Integer, String

from pgcraft.computed import PGCraftComputed, collect_computed


class TestPGCraftComputedColumnNames:
    def test_single_marker(self):
        comp = PGCraftComputed("full", "{first} || {last}", type=String())
        assert comp.column_names() == ["first", "last"]

    def test_duplicate_markers_deduplicated(self):
        comp = PGCraftComputed("dbl", "{x} + {x}", type=Integer())
        assert comp.column_names() == ["x"]

    def test_no_markers(self):
        comp = PGCraftComputed("lit", "'hello'", type=String())
        assert comp.column_names() == []


class TestPGCraftComputedResolve:
    def test_identity_mapping(self):
        comp = PGCraftComputed("full", "{first} || {last}", type=String())
        assert comp.resolve(lambda c: c) == "first || last"

    def test_alias_mapping(self):
        comp = PGCraftComputed("full", "{first} || {last}", type=String())
        result = comp.resolve(lambda c: f"p.{c}")
        assert result == "p.first || p.last"

    def test_duplicate_markers_all_resolved(self):
        comp = PGCraftComputed("dbl", "{x} + {x}", type=Integer())
        result = comp.resolve(lambda c: f"t.{c}")
        assert result == "t.x + t.x"


class TestPGCraftComputedFrozen:
    def test_is_immutable(self):
        comp = PGCraftComputed("a", "{x}", type=String())
        with pytest.raises(AttributeError):
            comp.name = "b"  # type: ignore[misc]


class TestCollectComputed:
    def test_filters_computed_from_mixed_list(self):
        items = [
            Column("first", String),
            PGCraftComputed("full", "{first} || {last}", type=String()),
            Column("last", String),
        ]
        result = collect_computed(items)
        assert len(result) == 1
        assert result[0].name == "full"

    def test_empty_list(self):
        assert collect_computed([]) == []

    def test_no_computed_in_list(self):
        assert collect_computed([Column("x", String)]) == []
