"""Unit tests for SerialPKPlugin in isolation."""

from sqlalchemy import Integer

from cave.plugins.pk import SerialPKPlugin
from tests.unit.plugins.conftest import make_ctx


class TestSerialPKPlugin:
    def test_returns_list(self):
        plugin = SerialPKPlugin()
        ctx = make_ctx()
        result = plugin.pk_columns(ctx)
        assert isinstance(result, list)

    def test_returns_one_column(self):
        plugin = SerialPKPlugin()
        ctx = make_ctx()
        assert len(plugin.pk_columns(ctx)) == 1

    def test_default_column_name_is_id(self):
        plugin = SerialPKPlugin()
        ctx = make_ctx()
        col = plugin.pk_columns(ctx)[0]
        assert col.key == "id"

    def test_custom_column_name(self):
        plugin = SerialPKPlugin(column_name="entity_id")
        ctx = make_ctx()
        col = plugin.pk_columns(ctx)[0]
        assert col.key == "entity_id"

    def test_column_type_is_integer(self):
        plugin = SerialPKPlugin()
        ctx = make_ctx()
        col = plugin.pk_columns(ctx)[0]
        assert isinstance(col.type, Integer)

    def test_column_is_primary_key(self):
        plugin = SerialPKPlugin()
        ctx = make_ctx()
        col = plugin.pk_columns(ctx)[0]
        assert col.primary_key is True

    def test_singleton_group_is_pk(self):
        assert SerialPKPlugin.singleton_group == "__pk__"
