"""Unit tests for CreatedAtPlugin."""

from sqlalchemy import Column, Integer, MetaData, String

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

    def test_does_not_inject_columns(self):
        # CreatedAtPlugin is a pure name-provider; each table plugin
        # that needs the column is responsible for constructing it.
        plugin = CreatedAtPlugin()
        ctx = _make_bare_ctx()
        plugin.run(ctx)
        assert ctx.injected_columns == []
