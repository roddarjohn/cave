"""Unit tests for PK plugins in isolation."""

import pytest
from sqlalchemy import Integer, MetaData
from sqlalchemy.dialects.postgresql import UUID

from pgcraft.columns import PrimaryKeyColumns
from pgcraft.errors import PGCraftValidationError
from pgcraft.factory.context import FactoryContext
from pgcraft.plugin import check_pg_version
from pgcraft.plugins.pk import (
    SerialPKPlugin,
    UUIDV4PKPlugin,
    UUIDV7PKPlugin,
)


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

    def test_column_has_server_default(self):
        plugin = UUIDV7PKPlugin()
        ctx = _bare_ctx()
        plugin.run(ctx)
        col = ctx["pk_columns"].first
        assert col.server_default is not None

    def test_server_default_uses_uuidv7(self):
        plugin = UUIDV7PKPlugin()
        ctx = _bare_ctx()
        plugin.run(ctx)
        col = ctx["pk_columns"].first
        assert "uuidv7()" in str(col.server_default.arg)

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

    def test_min_pg_version(self):
        assert UUIDV7PKPlugin.min_pg_version == 18

    def test_requires_does_not_include_pg_in_ctx_keys(self):
        plugin = UUIDV7PKPlugin()
        assert plugin.resolved_requires() == []


class TestCheckPGVersion:
    def test_passes_when_version_sufficient(self):
        check_pg_version(18, [UUIDV7PKPlugin()])

    def test_raises_when_version_too_low(self):
        with pytest.raises(
            PGCraftValidationError,
            match=r"UUIDV7PKPlugin requires PostgreSQL >= 18.*version 17",
        ):
            check_pg_version(17, [UUIDV7PKPlugin()])

    def test_ignores_plugins_without_version(self):
        check_pg_version(13, [SerialPKPlugin()])

    def test_mixed_plugins(self):
        plugins = [SerialPKPlugin(), UUIDV7PKPlugin()]
        check_pg_version(18, plugins)
        with pytest.raises(PGCraftValidationError):
            check_pg_version(17, plugins)
