"""Unit tests for CreatedAtPlugin."""

from sqlalchemy import Column, DateTime, Integer, MetaData, String

from pgcraft.columns import PrimaryKeyColumns
from pgcraft.factory.context import FactoryContext
from pgcraft.plugins.created_at import CreatedAtPlugin


def _make_bare_ctx() -> FactoryContext:
    """Return a FactoryContext without created_at_column pre-set."""
    ctx = FactoryContext(
        tablename="product",
        schemaname="dim",
        metadata=MetaData(),
        schema_items=[Column("name", String)],
        plugins=[],
    )
    ctx["pk_columns"] = PrimaryKeyColumns(
        [Column("id", Integer, primary_key=True)]
    )
    return ctx


class TestCreatedAtPlugin:
    def test_stores_default_column_name(self):
        plugin = CreatedAtPlugin()
        ctx = _make_bare_ctx()
        plugin.run(ctx)
        assert ctx["created_at_column"] == "created_at"

    def test_stores_custom_column_name(self):
        plugin = CreatedAtPlugin(column_name="inserted_at")
        ctx = _make_bare_ctx()
        plugin.run(ctx)
        assert ctx["created_at_column"] == "inserted_at"

    def test_produces_created_at_column(self):
        plugin = CreatedAtPlugin()
        assert "created_at_column" in plugin.resolved_produces()

    def test_injects_column(self):
        plugin = CreatedAtPlugin()
        ctx = _make_bare_ctx()
        plugin.run(ctx)
        assert len(ctx.injected_columns) == 1
        col = ctx.injected_columns[0]
        assert col.name == "created_at"
        assert isinstance(col.type, DateTime)

    def test_injects_custom_named_column(self):
        plugin = CreatedAtPlugin(column_name="inserted_at")
        ctx = _make_bare_ctx()
        plugin.run(ctx)
        assert ctx.injected_columns[0].name == "inserted_at"
