"""Unit tests for pgcraft.index module."""

import pytest

from pgcraft.index import PGCraftIndex, collect_indices


class TestPGCraftIndex:
    def test_basic_construction(self):
        idx = PGCraftIndex(columns=["price"], name="idx_price")
        assert idx.columns == ["price"]
        assert idx.name == "idx_price"
        assert idx.unique is False

    def test_unique_flag(self):
        idx = PGCraftIndex(
            columns=["code"],
            name="uq_code",
            unique=True,
        )
        assert idx.unique is True

    def test_multi_column(self):
        idx = PGCraftIndex(
            columns=["a", "b"],
            name="idx_ab",
        )
        assert idx.columns == ["a", "b"]

    def test_is_frozen(self):
        idx = PGCraftIndex(columns=["a"], name="idx_a")
        with pytest.raises(AttributeError):
            idx.name = "changed"  # type: ignore[misc]


class TestCollectIndices:
    def test_filters_indices_from_mixed_list(self):
        from sqlalchemy import Column, Integer

        items = [
            Column("price", Integer),
            PGCraftIndex(columns=["price"], name="idx_price"),
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
