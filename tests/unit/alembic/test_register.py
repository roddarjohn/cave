"""Unit tests for pgcraft.alembic.register."""

from sqlalchemy import Column, Integer, MetaData, Table

from pgcraft.alembic.register import (
    pgcraft_alembic_hook,
    pgcraft_configure_metadata,
)
from pgcraft.config import PGCraftConfig
from pgcraft.extensions.postgrest import PostgRESTExtension


class TestCaveAlembicHook:
    def test_runs_without_error(self):
        """pgcraft_alembic_hook must not raise on repeated calls."""
        pgcraft_alembic_hook()

    def test_idempotent(self):
        """Calling the hook twice must not raise."""
        pgcraft_alembic_hook()
        pgcraft_alembic_hook()

    def test_patches_applied(self):
        """View.render_definition should be monkey-patched after the hook."""
        from sqlalchemy_declarative_extensions.view.base import View

        from pgcraft.patches.view_render import _patched_render_definition

        pgcraft_alembic_hook()
        assert View.render_definition is _patched_render_definition

    def test_extension_configure_alembic_called(self):
        called = []

        class _Ext(PostgRESTExtension):
            def configure_alembic(self):
                called.append(True)

        config = PGCraftConfig(
            auto_discover=False,
            extensions=[_Ext()],
        )
        pgcraft_alembic_hook(config)
        assert called == [True]


class TestPGCraftConfigureMetadata:
    def test_no_roles_without_extension(self):
        """Without PostgREST extension, no roles are registered."""
        metadata = MetaData()
        config = PGCraftConfig(auto_discover=False)
        pgcraft_configure_metadata(metadata, config)
        assert "roles" not in metadata.info

    def test_registers_roles_with_postgrest_extension(self):
        """With PostgREST extension, roles are registered."""
        metadata = MetaData()
        config = PGCraftConfig(auto_discover=False)
        config.use(PostgRESTExtension())
        pgcraft_configure_metadata(metadata, config)
        assert "roles" in metadata.info

    def test_registers_grants_with_postgrest_extension(self):
        metadata = MetaData()
        config = PGCraftConfig(auto_discover=False)
        config.use(PostgRESTExtension())
        pgcraft_configure_metadata(metadata, config)
        assert "grants" in metadata.info

    def test_no_schemas_without_tables(self):
        """With an empty MetaData, 'schemas' should not appear."""
        metadata = MetaData()
        config = PGCraftConfig(auto_discover=False)
        pgcraft_configure_metadata(metadata, config)
        assert "schemas" not in metadata.info

    def test_registers_schemas_when_tables_present(self):
        """Non-system schema tables must trigger schema registration."""
        metadata = MetaData()
        Table("t", metadata, Column("id", Integer), schema="dim")
        config = PGCraftConfig(auto_discover=False)
        pgcraft_configure_metadata(metadata, config)
        assert "schemas" in metadata.info

    def test_registered_schema_name_correct(self):
        metadata = MetaData()
        Table("t", metadata, Column("id", Integer), schema="dim")
        config = PGCraftConfig(auto_discover=False)
        pgcraft_configure_metadata(metadata, config)
        schema_names = {s.name for s in metadata.info["schemas"].schemas}
        assert "dim" in schema_names

    def test_authenticator_role_with_extension(self):
        metadata = MetaData()
        config = PGCraftConfig(auto_discover=False)
        config.use(PostgRESTExtension())
        pgcraft_configure_metadata(metadata, config)
        roles = metadata.info["roles"]
        names = {r.name for r in roles.roles}
        assert "authenticator" in names

    def test_anon_role_with_extension(self):
        metadata = MetaData()
        config = PGCraftConfig(auto_discover=False)
        config.use(PostgRESTExtension())
        pgcraft_configure_metadata(metadata, config)
        roles = metadata.info["roles"]
        names = {r.name for r in roles.roles}
        assert "anon" in names

    def test_config_from_metadata_info(self):
        """Falls back to pgcraft_config stored on metadata.info."""
        config = PGCraftConfig(auto_discover=False)
        config.use(PostgRESTExtension())
        metadata = MetaData(info={"pgcraft_config": config})
        pgcraft_configure_metadata(metadata)
        assert "roles" in metadata.info
