"""Unit tests for UUIDEntryIDPlugin."""

from sqlalchemy import Column
from sqlalchemy.dialects.postgresql import UUID

from pgcraft.plugins.entry_id import UUIDEntryIDPlugin
from tests.unit.plugins.conftest import make_ctx


class TestUUIDEntryIDPlugin:
    def test_stores_entry_id_column(self):
        plugin = UUIDEntryIDPlugin()
        ctx = make_ctx()
        plugin.run(ctx)
        col = ctx["entry_id_column"]
        assert isinstance(col, Column)
        assert col.name == "entry_id"

    def test_column_is_uuid_type(self):
        plugin = UUIDEntryIDPlugin()
        ctx = make_ctx()
        plugin.run(ctx)
        col = ctx["entry_id_column"]
        assert isinstance(col.type, UUID)

    def test_column_is_not_nullable(self):
        plugin = UUIDEntryIDPlugin()
        ctx = make_ctx()
        plugin.run(ctx)
        col = ctx["entry_id_column"]
        assert not col.nullable

    def test_column_has_server_default(self):
        plugin = UUIDEntryIDPlugin()
        ctx = make_ctx()
        plugin.run(ctx)
        col = ctx["entry_id_column"]
        assert col.server_default is not None

    def test_custom_column_name(self):
        plugin = UUIDEntryIDPlugin(column_name="correlation_id")
        ctx = make_ctx()
        plugin.run(ctx)
        col = ctx["entry_id_column"]
        assert col.name == "correlation_id"

    def test_singleton_group(self):
        assert UUIDEntryIDPlugin.singleton_group == "__entry_id__"

    def test_produces_entry_id_column(self):
        plugin = UUIDEntryIDPlugin()
        assert "entry_id_column" in plugin.resolved_produces()

    def test_injects_column(self):
        plugin = UUIDEntryIDPlugin()
        ctx = make_ctx()
        plugin.run(ctx)
        injected_names = [c.name for c in ctx.injected_columns]
        assert "entry_id" in injected_names

    def test_injected_column_is_same_object(self):
        plugin = UUIDEntryIDPlugin()
        ctx = make_ctx()
        plugin.run(ctx)
        entry_col = next(
            c for c in ctx.injected_columns if c.name == "entry_id"
        )
        assert entry_col is ctx["entry_id_column"]
