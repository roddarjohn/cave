"""Unit tests for PK plugins in isolation."""

from sqlalchemy import Integer, MetaData
from sqlalchemy.dialects.postgresql import UUID

from pgcraft.columns import PrimaryKeyColumns
from pgcraft.factory.base import _collect_extensions
from pgcraft.factory.context import FactoryContext
from pgcraft.plugins.pk import SerialPKPlugin, UUIDV4PKPlugin, UUIDV7PKPlugin


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


class TestUUIDV4PKPlugin:
    def test_run_stores_primary_key_columns(self):
        plugin = UUIDV4PKPlugin()
        ctx = _bare_ctx()
        plugin.run(ctx)
        assert isinstance(ctx["pk_columns"], PrimaryKeyColumns)

    def test_default_column_name_is_id(self):
        plugin = UUIDV4PKPlugin()
        ctx = _bare_ctx()
        plugin.run(ctx)
        assert ctx["pk_columns"].first_key == "id"

    def test_custom_column_name(self):
        plugin = UUIDV4PKPlugin(column_name="entity_id")
        ctx = _bare_ctx()
        plugin.run(ctx)
        assert ctx["pk_columns"].first_key == "entity_id"

    def test_column_type_is_uuid(self):
        plugin = UUIDV4PKPlugin()
        ctx = _bare_ctx()
        plugin.run(ctx)
        col = ctx["pk_columns"].first
        assert isinstance(col.type, UUID)

    def test_column_is_primary_key(self):
        plugin = UUIDV4PKPlugin()
        ctx = _bare_ctx()
        plugin.run(ctx)
        col = ctx["pk_columns"].first
        assert col.primary_key is True

    def test_column_has_server_default(self):
        plugin = UUIDV4PKPlugin()
        ctx = _bare_ctx()
        plugin.run(ctx)
        col = ctx["pk_columns"].first
        assert col.server_default is not None

    def test_singleton_group_is_pk(self):
        assert UUIDV4PKPlugin.singleton_group == "__pk__"

    def test_produces_pk_columns(self):
        plugin = UUIDV4PKPlugin()
        assert plugin.resolved_produces() == ["pk_columns"]

    def test_len_is_one(self):
        plugin = UUIDV4PKPlugin()
        ctx = _bare_ctx()
        plugin.run(ctx)
        assert len(ctx["pk_columns"]) == 1

    def test_no_required_extensions(self):
        assert UUIDV4PKPlugin.required_pg_extensions == frozenset()


class TestUUIDV7PKPlugin:
    def test_run_stores_primary_key_columns(self):
        plugin = UUIDV7PKPlugin()
        ctx = _bare_ctx()
        plugin.run(ctx)
        assert isinstance(ctx["pk_columns"], PrimaryKeyColumns)

    def test_default_column_name_is_id(self):
        plugin = UUIDV7PKPlugin()
        ctx = _bare_ctx()
        plugin.run(ctx)
        assert ctx["pk_columns"].first_key == "id"

    def test_custom_column_name(self):
        plugin = UUIDV7PKPlugin(column_name="entity_id")
        ctx = _bare_ctx()
        plugin.run(ctx)
        assert ctx["pk_columns"].first_key == "entity_id"

    def test_column_type_is_uuid(self):
        plugin = UUIDV7PKPlugin()
        ctx = _bare_ctx()
        plugin.run(ctx)
        col = ctx["pk_columns"].first
        assert isinstance(col.type, UUID)

    def test_column_is_primary_key(self):
        plugin = UUIDV7PKPlugin()
        ctx = _bare_ctx()
        plugin.run(ctx)
        col = ctx["pk_columns"].first
        assert col.primary_key is True

    def test_server_default_is_uuid_generate_v7(self):
        plugin = UUIDV7PKPlugin()
        ctx = _bare_ctx()
        plugin.run(ctx)
        col = ctx["pk_columns"].first
        assert col.server_default is not None
        assert "uuid_generate_v7" in str(col.server_default.arg)

    def test_singleton_group_is_pk(self):
        assert UUIDV7PKPlugin.singleton_group == "__pk__"

    def test_produces_pk_columns(self):
        plugin = UUIDV7PKPlugin()
        assert plugin.resolved_produces() == ["pk_columns"]

    def test_len_is_one(self):
        plugin = UUIDV7PKPlugin()
        ctx = _bare_ctx()
        plugin.run(ctx)
        assert len(ctx["pk_columns"]) == 1

    def test_requires_pg_uuidv7_extension(self):
        assert "pg_uuidv7" in UUIDV7PKPlugin.required_pg_extensions

    def test_collect_extensions_populates_metadata(self):
        metadata = MetaData()
        _collect_extensions([UUIDV7PKPlugin()], metadata)
        assert "pg_uuidv7" in metadata.info["pgcraft_extensions"]

    def test_collect_extensions_merges_across_calls(self):
        # Two factories sharing the same MetaData both register extensions.
        metadata = MetaData()
        _collect_extensions([UUIDV7PKPlugin()], metadata)
        _collect_extensions([UUIDV7PKPlugin()], metadata)
        assert metadata.info["pgcraft_extensions"] == {"pg_uuidv7"}

    def test_collect_extensions_no_entry_for_serial_plugin(self):
        metadata = MetaData()
        _collect_extensions([SerialPKPlugin()], metadata)
        assert "pgcraft_extensions" not in metadata.info
