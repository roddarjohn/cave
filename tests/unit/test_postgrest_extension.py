"""Unit tests for PostgRESTExtension."""

from sqlalchemy import MetaData

from pgcraft.config import PGCraftConfig
from pgcraft.extensions.postgrest import PostgRESTExtension
from pgcraft.resource import APIResource, register_api_resource


class TestPostgRESTExtensionDefaults:
    def test_name(self):
        ext = PostgRESTExtension()
        assert ext.name == "postgrest"

    def test_schema_default(self):
        ext = PostgRESTExtension()
        assert ext.schema == "api"

    def test_plugins_empty(self):
        ext = PostgRESTExtension()
        assert ext.plugins() == []


class TestPostgRESTConfigureMetadata:
    def test_registers_roles(self):
        ext = PostgRESTExtension()
        metadata = MetaData()
        ext.configure_metadata(metadata)
        assert "roles" in metadata.info
        names = {r.name for r in metadata.info["roles"].roles}
        assert "authenticator" in names
        assert "anon" in names

    def test_registers_grants(self):
        ext = PostgRESTExtension()
        metadata = MetaData()
        ext.configure_metadata(metadata)
        assert "grants" in metadata.info

    def test_grants_include_api_resources(self):
        ext = PostgRESTExtension()
        metadata = MetaData()
        register_api_resource(
            metadata,
            APIResource("products", schema="api", grants=["select"]),
        )
        ext.configure_metadata(metadata)
        grants_text = str(metadata.info["grants"])
        assert "products" in grants_text.lower()


class TestPostgRESTViaConfig:
    def test_configure_metadata_via_config(self):
        config = PGCraftConfig(auto_discover=False)
        config.use(PostgRESTExtension())
        metadata = MetaData()
        for ext in config._resolved_extensions():
            ext.configure_metadata(metadata)
        assert "roles" in metadata.info

    def test_no_roles_without_extension(self):
        config = PGCraftConfig(auto_discover=False)
        metadata = MetaData()
        for ext in config._resolved_extensions():
            ext.configure_metadata(metadata)
        assert "roles" not in metadata.info
