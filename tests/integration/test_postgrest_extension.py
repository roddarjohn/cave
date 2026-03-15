"""Integration tests for PostgRESTExtension with live metadata."""

from sqlalchemy import Column, Integer, MetaData, String

from pgcraft.alembic.register import pgcraft_configure_metadata
from pgcraft.config import PGCraftConfig
from pgcraft.extensions.postgrest import PostgRESTExtension, PostgRESTView
from pgcraft.factory.dimension.simple import PGCraftSimple
from pgcraft.resource import APIResource, register_api_resource


class TestPostgRESTExtensionIntegration:
    def test_full_factory_with_api_view_produces_grants(self):
        """Factory -> PostgRESTView -> PostgREST extension registers grants."""
        config = PGCraftConfig(auto_discover=False)
        config.use(PostgRESTExtension())

        metadata = MetaData()
        factory = PGCraftSimple(
            "items",
            "public",
            metadata,
            schema_items=[
                Column("name", String),
                Column("price", Integer),
            ],
            config=config,
        )

        PostgRESTView(
            source=factory,
            grants=["select", "insert"],
        )

        pgcraft_configure_metadata(metadata, config)

        assert "roles" in metadata.info
        assert "grants" in metadata.info

        grants_text = str(metadata.info["grants"])
        assert "items" in grants_text.lower()

    def test_no_grants_without_extension(self):
        """Without PostgREST extension, no roles or grants."""
        config = PGCraftConfig(auto_discover=False)

        metadata = MetaData()
        factory = PGCraftSimple(
            "items",
            "public",
            metadata,
            schema_items=[Column("name", String)],
            config=config,
        )

        PostgRESTView(source=factory)

        pgcraft_configure_metadata(metadata, config)

        assert "roles" not in metadata.info

    def test_multiple_resources_via_extension(self):
        config = PGCraftConfig(auto_discover=False)
        config.use(PostgRESTExtension())

        metadata = MetaData()
        register_api_resource(
            metadata,
            APIResource("a", schema="api", grants=["select"]),
        )
        register_api_resource(
            metadata,
            APIResource("b", schema="api", grants=["select"]),
        )

        pgcraft_configure_metadata(metadata, config)

        grants = metadata.info["grants"]
        grants_text = str(grants)
        assert "a" in grants_text.lower()
        assert "b" in grants_text.lower()
