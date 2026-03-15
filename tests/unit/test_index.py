"""Unit tests for pgcraft.index module."""

import pytest

from pgcraft.index import PGCraftIndex, collect_indices


class TestPGCraftIndex:
    def test_basic_construction(self):
        idx = PGCraftIndex("idx_price", "{price}")
        assert idx.name == "idx_price"
        assert idx.expressions == ["{price}"]
        assert idx.unique is False
        assert idx.kw == {}

    def test_unique_flag(self):
        idx = PGCraftIndex("uq_code", "{code}", unique=True)
        assert idx.unique is True

    def test_multi_column(self):
        idx = PGCraftIndex("idx_ab", "{a}", "{b}")
        assert idx.expressions == ["{a}", "{b}"]

    def test_is_immutable(self):
        idx = PGCraftIndex("idx_a", "{a}")
        with pytest.raises(AttributeError):
            idx.name = "changed"  # type: ignore[misc]

    def test_kwargs_passthrough(self):
        idx = PGCraftIndex(
            "idx_gin",
            "{data}",
            postgresql_using="gin",
        )
        assert idx.kw == {"postgresql_using": "gin"}

    def test_column_names(self):
        idx = PGCraftIndex("idx_ab", "{a}", "{b}")
        assert idx.column_names() == ["a", "b"]

    def test_column_names_deduplicates(self):
        idx = PGCraftIndex("idx_aa", "{a}", "{a}")
        assert idx.column_names() == ["a"]

    def test_column_names_functional(self):
        idx = PGCraftIndex("idx_lower", "lower({name})")
        assert idx.column_names() == ["name"]

    def test_resolve_identity(self):
        idx = PGCraftIndex("idx", "{a}", "{b}")
        assert idx.resolve(lambda c: c) == ["a", "b"]

    def test_resolve_functional(self):
        idx = PGCraftIndex("idx", "lower({name})")
        assert idx.resolve(lambda c: c) == ["lower(name)"]

    def test_repr(self):
        idx = PGCraftIndex("idx", "{a}", unique=True)
        r = repr(idx)
        assert "idx" in r
        assert "{a}" in r
        assert "unique=True" in r

    def test_eq(self):
        a = PGCraftIndex("idx", "{a}", unique=True)
        b = PGCraftIndex("idx", "{a}", unique=True)
        assert a == b

    def test_not_eq(self):
        a = PGCraftIndex("idx", "{a}")
        b = PGCraftIndex("idx", "{b}")
        assert a != b


class TestCollectIndices:
    def test_filters_indices_from_mixed_list(self):
        from sqlalchemy import Column, Integer

        items = [
            Column("price", Integer),
            PGCraftIndex("idx_price", "{price}"),
            Column("qty", Integer),
        ]
        result = collect_indices(items)
        assert len(result) == 1
        assert result[0].name == "idx_price"

    def test_empty_list(self):
        assert collect_indices([]) == []

    def test_no_indices_in_list(self):
        from sqlalchemy import Column, String

        assert collect_indices([Column("x", String)]) == []
