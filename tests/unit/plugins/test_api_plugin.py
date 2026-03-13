"""Unit tests for APIPlugin in isolation."""

import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table

from pgcraft.plugins.api import APIPlugin
from pgcraft.statistics import StatisticsViewInfo
from tests.unit.plugins.conftest import make_ctx


def _ctx_with_primary(
    schema: str = "dim",
    table_key: str = "primary",
    store=None,
):
    """Return a ctx with a simple Table pre-stored at table_key."""
    ctx = make_ctx(schemaname=schema, store=store)
    table = Table(
        "product",
        MetaData(),
        Column("id", Integer, primary_key=True),
        Column("name", String),
        schema=schema,
    )
    ctx[table_key] = table
    return ctx


class TestAPIPlugin:
    def test_view_registered_in_metadata(self):
        plugin = APIPlugin()
        ctx = _ctx_with_primary()
        plugin.run(ctx)
        views = ctx.metadata.info.get("views")
        assert views is not None
        assert len(views.views) == 1

    def test_view_name_matches_tablename(self):
        plugin = APIPlugin()
        ctx = _ctx_with_primary()
        plugin.run(ctx)
        view = ctx.metadata.info["views"].views[0]
        assert view.name == "product"

    def test_view_schema_defaults_to_api(self):
        plugin = APIPlugin()
        ctx = _ctx_with_primary()
        plugin.run(ctx)
        view = ctx.metadata.info["views"].views[0]
        assert view.schema == "api"

    def test_custom_schema(self):
        plugin = APIPlugin(schema="public_api")
        ctx = _ctx_with_primary()
        plugin.run(ctx)
        view = ctx.metadata.info["views"].views[0]
        assert view.schema == "public_api"

    def test_view_stored_under_default_view_key(self):
        plugin = APIPlugin()
        ctx = _ctx_with_primary()
        plugin.run(ctx)
        assert "api" in ctx

    def test_custom_view_key(self):
        plugin = APIPlugin(view_key="my_api_view")
        ctx = _ctx_with_primary()
        plugin.run(ctx)
        assert "my_api_view" in ctx

    def test_custom_table_key(self):
        plugin = APIPlugin(table_key="source")
        ctx = _ctx_with_primary(table_key="source")
        plugin.run(ctx)
        assert "api" in ctx

    def test_view_definition_references_source_table(self):
        plugin = APIPlugin()
        ctx = _ctx_with_primary()
        plugin.run(ctx)
        view = ctx.metadata.info["views"].views[0]
        assert "product" in view.definition

    def test_registers_api_resource(self):
        plugin = APIPlugin()
        ctx = _ctx_with_primary()
        plugin.run(ctx)
        resources = ctx.metadata.info.get("api_resources", [])
        assert len(resources) == 1

    def test_resource_name_matches_tablename(self):
        plugin = APIPlugin()
        ctx = _ctx_with_primary()
        plugin.run(ctx)
        assert ctx.metadata.info["api_resources"][0].name == "product"

    def test_resource_schema_matches_plugin_schema(self):
        plugin = APIPlugin(schema="reporting")
        ctx = _ctx_with_primary()
        plugin.run(ctx)
        assert ctx.metadata.info["api_resources"][0].schema == "reporting"


class TestAPIPluginColumns:
    def test_columns_none_selects_all(self):
        plugin = APIPlugin()
        ctx = _ctx_with_primary()
        plugin.run(ctx)
        view = ctx.metadata.info["views"].views[0]
        assert "id" in view.definition
        assert "name" in view.definition

    def test_columns_subset(self):
        plugin = APIPlugin(columns=["id"])
        ctx = _ctx_with_primary()
        plugin.run(ctx)
        view = ctx.metadata.info["views"].views[0]
        assert "p.id" in view.definition
        assert "p.name" not in view.definition

    def test_columns_unknown_raises(self):
        plugin = APIPlugin(columns=["id", "nonexistent"])
        ctx = _ctx_with_primary()
        with pytest.raises(ValueError, match="nonexistent"):
            plugin.run(ctx)


class TestAPIPluginStatsJoin:
    def _ctx_with_stats(self):
        stats_info = {
            "statistics": StatisticsViewInfo(
                view_name="dim.product_statistics",
                join_key="id",
                column_names=["order_count"],
            ),
        }
        return _ctx_with_primary(
            store={"statistics_views": stats_info},
        )

    def test_stats_join_in_definition(self):
        plugin = APIPlugin()
        ctx = self._ctx_with_stats()
        plugin.run(ctx)
        view = ctx.metadata.info["views"].views[0]
        defn = view.definition.lower()
        assert "left outer join" in defn
        assert "product_statistics" in defn

    def test_stats_columns_in_select(self):
        plugin = APIPlugin()
        ctx = self._ctx_with_stats()
        plugin.run(ctx)
        view = ctx.metadata.info["views"].views[0]
        assert "order_count" in view.definition

    def test_stats_with_column_selection(self):
        stats_info = {
            "statistics": StatisticsViewInfo(
                view_name="dim.product_statistics",
                join_key="id",
                column_names=["order_count"],
            ),
        }
        plugin = APIPlugin(columns=["id"])
        ctx = _ctx_with_primary(
            store={"statistics_views": stats_info},
        )
        plugin.run(ctx)
        view = ctx.metadata.info["views"].views[0]
        assert "p.id" in view.definition
        assert "order_count" in view.definition

    def test_empty_stats_no_join(self):
        plugin = APIPlugin()
        ctx = _ctx_with_primary(
            store={"statistics_views": {}},
        )
        plugin.run(ctx)
        view = ctx.metadata.info["views"].views[0]
        assert "join" not in view.definition.lower()

    def test_missing_stats_key_no_join(self):
        plugin = APIPlugin()
        ctx = _ctx_with_primary()
        plugin.run(ctx)
        view = ctx.metadata.info["views"].views[0]
        assert "join" not in view.definition.lower()
