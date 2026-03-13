"""Unit tests for SerialPKPlugin in isolation."""

from sqlalchemy import Integer, MetaData

from cave.columns import PrimaryKeyColumns
from cave.factory.context import FactoryContext
from cave.plugins.pk import SerialPKPlugin


def _bare_ctx() -> FactoryContext:
    """Return a minimal FactoryContext with no pk_columns set."""
    return FactoryContext(
        tablename="t",
        schemaname="s",
        metadata=MetaData(),
        schema_items=[],
        plugins=[],
    )


class TestSerialPKPlugin:
    def test_run_stores_primary_key_columns(self):
        plugin = SerialPKPlugin()
        ctx = _bare_ctx()
        plugin.run(ctx)
        assert isinstance(ctx["pk_columns"], PrimaryKeyColumns)

    def test_default_column_name_is_id(self):
        plugin = SerialPKPlugin()
        ctx = _bare_ctx()
        plugin.run(ctx)
        assert ctx["pk_columns"].first_key == "id"

    def test_custom_column_name(self):
        plugin = SerialPKPlugin(column_name="entity_id")
        ctx = _bare_ctx()
        plugin.run(ctx)
        assert ctx["pk_columns"].first_key == "entity_id"

    def test_column_type_is_integer(self):
        plugin = SerialPKPlugin()
        ctx = _bare_ctx()
        plugin.run(ctx)
        col = ctx["pk_columns"].first
        assert isinstance(col.type, Integer)

    def test_column_is_primary_key(self):
        plugin = SerialPKPlugin()
        ctx = _bare_ctx()
        plugin.run(ctx)
        col = ctx["pk_columns"].first
        assert col.primary_key is True

    def test_singleton_group_is_pk(self):
        assert SerialPKPlugin.singleton_group == "__pk__"

    def test_produces_pk_columns(self):
        plugin = SerialPKPlugin()
        assert plugin.resolved_produces() == ["pk_columns"]

    def test_len_is_one(self):
        plugin = SerialPKPlugin()
        ctx = _bare_ctx()
        plugin.run(ctx)
        assert len(ctx["pk_columns"]) == 1
