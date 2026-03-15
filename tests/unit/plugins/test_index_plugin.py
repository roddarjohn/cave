"""Unit tests for TableIndexPlugin."""

import pytest
from sqlalchemy import Column, Integer, String, Table

from pgcraft.errors import PGCraftValidationError
from pgcraft.index import PGCraftIndex
from pgcraft.plugins.index import TableIndexPlugin
from tests.unit.plugins.conftest import make_ctx


class TestTableIndexPlugin:
    def test_creates_index_on_table(self):
        from pgcraft.plugins.simple import SimpleTablePlugin

        ctx = make_ctx(
            schema_items=[
                Column("price", Integer),
                PGCraftIndex(
                    columns=["price"],
                    name="idx_price",
                ),
            ]
        )
        SimpleTablePlugin().run(ctx)
        TableIndexPlugin().run(ctx)
        table = ctx["primary"]
        idx_names = [i.name for i in table.indexes]
        assert "idx_price" in idx_names

    def test_unique_index(self):
        from pgcraft.plugins.simple import SimpleTablePlugin

        ctx = make_ctx(
            schema_items=[
                Column("code", String),
                PGCraftIndex(
                    columns=["code"],
                    name="uq_code",
                    unique=True,
                ),
            ]
        )
        SimpleTablePlugin().run(ctx)
        TableIndexPlugin().run(ctx)
        table = ctx["primary"]
        idx = next(i for i in table.indexes if i.name == "uq_code")
        assert idx.unique is True

    def test_multi_column_index(self):
        from pgcraft.plugins.simple import SimpleTablePlugin

        ctx = make_ctx(
            schema_items=[
                Column("a", Integer),
                Column("b", Integer),
                PGCraftIndex(
                    columns=["a", "b"],
                    name="idx_ab",
                ),
            ]
        )
        SimpleTablePlugin().run(ctx)
        TableIndexPlugin().run(ctx)
        table = ctx["primary"]
        idx_names = [i.name for i in table.indexes]
        assert "idx_ab" in idx_names

    def test_no_indices_is_noop(self):
        from pgcraft.plugins.simple import SimpleTablePlugin

        ctx = make_ctx(schema_items=[Column("name", String)])
        SimpleTablePlugin().run(ctx)
        TableIndexPlugin().run(ctx)
        assert isinstance(ctx["primary"], Table)

    def test_unknown_column_raises(self):
        from pgcraft.plugins.simple import SimpleTablePlugin

        ctx = make_ctx(
            schema_items=[
                Column("price", Integer),
                PGCraftIndex(
                    columns=["nonexistent"],
                    name="idx_bad",
                ),
            ]
        )
        SimpleTablePlugin().run(ctx)
        with pytest.raises(
            PGCraftValidationError,
            match="nonexistent",
        ):
            TableIndexPlugin().run(ctx)

    def test_custom_table_key(self):
        from pgcraft.plugins.simple import SimpleTablePlugin

        ctx = make_ctx(
            schema_items=[
                Column("price", Integer),
                PGCraftIndex(
                    columns=["price"],
                    name="idx_price",
                ),
            ]
        )
        SimpleTablePlugin(table_key="my_table").run(ctx)
        TableIndexPlugin(table_key="my_table").run(ctx)
        table = ctx["my_table"]
        idx_names = [i.name for i in table.indexes]
        assert "idx_price" in idx_names

    def test_requires_dynamic_table_key(self):
        plugin = TableIndexPlugin()
        assert "primary" in plugin.resolved_requires()
