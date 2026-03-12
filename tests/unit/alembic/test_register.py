"""Unit tests for cave.alembic.register."""

from sqlalchemy import Column, Integer, MetaData, Table

from cave.alembic.register import cave_alembic_hook, cave_configure_metadata


class TestCaveAlembicHook:
    def test_runs_without_error(self):
        """cave_alembic_hook must not raise on repeated calls."""
        cave_alembic_hook()

    def test_idempotent(self):
        """Calling the hook twice must not raise."""
        cave_alembic_hook()
        cave_alembic_hook()

    def test_patches_applied(self):
        """View.render_definition should be monkey-patched after the hook."""
        from sqlalchemy_declarative_extensions.view.base import View

        from cave.patches.view_render import _patched_render_definition

        cave_alembic_hook()
        assert View.render_definition is _patched_render_definition


class TestCaveConfigureMetadata:
    def test_registers_roles(self):
        """metadata.info must contain 'roles' after the call."""
        metadata = MetaData()
        cave_configure_metadata(metadata)
        assert "roles" in metadata.info

    def test_registers_grants(self):
        """metadata.info must contain 'grants' after the call."""
        metadata = MetaData()
        cave_configure_metadata(metadata)
        assert "grants" in metadata.info

    def test_no_schemas_without_tables(self):
        """With an empty MetaData, 'schemas' should not appear."""
        metadata = MetaData()
        cave_configure_metadata(metadata)
        assert "schemas" not in metadata.info

    def test_registers_schemas_when_tables_present(self):
        """Non-system schema tables must trigger schema registration."""
        metadata = MetaData()
        Table("t", metadata, Column("id", Integer), schema="dim")
        cave_configure_metadata(metadata)
        assert "schemas" in metadata.info

    def test_registered_schema_name_correct(self):
        metadata = MetaData()
        Table("t", metadata, Column("id", Integer), schema="dim")
        cave_configure_metadata(metadata)
        schema_names = {s.name for s in metadata.info["schemas"].schemas}
        assert "dim" in schema_names

    def test_authenticator_role_registered(self):
        metadata = MetaData()
        cave_configure_metadata(metadata)
        roles = metadata.info["roles"]
        names = {r.name for r in roles.roles}
        assert "authenticator" in names

    def test_anon_role_registered(self):
        metadata = MetaData()
        cave_configure_metadata(metadata)
        roles = metadata.info["roles"]
        names = {r.name for r in roles.roles}
        assert "anon" in names
