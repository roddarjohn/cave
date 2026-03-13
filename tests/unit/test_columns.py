"""Unit tests for PrimaryKeyColumns wrapper."""

import pytest
from sqlalchemy import Column, Integer

from pgcraft.columns import PrimaryKeyColumns


class TestPrimaryKeyColumns:
    def test_first_key_returns_column_key(self):
        pk = PrimaryKeyColumns([Column("uid", Integer, primary_key=True)])
        assert pk.first_key == "uid"

    def test_first_key_defaults_to_id_when_empty(self):
        pk = PrimaryKeyColumns([])
        assert pk.first_key == "id"

    def test_first_returns_column(self):
        col = Column("id", Integer, primary_key=True)
        pk = PrimaryKeyColumns([col])
        assert pk.first is col

    def test_first_raises_on_empty(self):
        pk = PrimaryKeyColumns([])
        with pytest.raises(IndexError):
            pk.first  # noqa: B018

    def test_iter_yields_all_columns(self):
        cols = [
            Column("a", Integer, primary_key=True),
            Column("b", Integer, primary_key=True),
        ]
        pk = PrimaryKeyColumns(cols)
        assert list(pk) == cols

    def test_len(self):
        pk = PrimaryKeyColumns([Column("id", Integer, primary_key=True)])
        assert len(pk) == 1

    def test_len_empty(self):
        pk = PrimaryKeyColumns([])
        assert len(pk) == 0

    def test_unpack_into_table_args(self):
        """PrimaryKeyColumns can be unpacked with * operator."""
        col = Column("id", Integer, primary_key=True)
        pk = PrimaryKeyColumns([col])
        unpacked = [*pk]
        assert unpacked == [col]

    def test_repr(self):
        pk = PrimaryKeyColumns([])
        assert "PrimaryKeyColumns" in repr(pk)
