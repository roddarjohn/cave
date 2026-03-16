"""Unit tests for AppendOnly{Table,View,Trigger}Plugin in isolation."""

from sqlalchemy import Column, String, Table

from pgcraft.plugins.append_only import (
    AppendOnlyTablePlugin,
    AppendOnlyViewPlugin,
    append_only_trigger_plugin,
)
from tests.unit.plugins.conftest import make_ctx, make_view


class TestAppendOnlyTablePlugin:
    def test_attributes_table_stored_under_default_key(self):
        plugin = AppendOnlyTablePlugin()
        ctx = make_ctx()
        plugin.run(ctx)
        assert isinstance(ctx["attributes"], Table)

    def test_root_table_stored_under_default_key(self):
        plugin = AppendOnlyTablePlugin()
        ctx = make_ctx()
        plugin.run(ctx)
        assert isinstance(ctx["root_table"], Table)

    def test_attributes_table_name(self):
        plugin = AppendOnlyTablePlugin()
        ctx = make_ctx()
        plugin.run(ctx)
        assert ctx["attributes"].name == "product_attributes"

    def test_root_table_name(self):
        plugin = AppendOnlyTablePlugin()
        ctx = make_ctx()
        plugin.run(ctx)
        assert ctx["root_table"].name == "product_root"

    def test_attributes_table_has_user_dimensions(self):
        plugin = AppendOnlyTablePlugin()
        ctx = make_ctx(schema_items=[Column("name", String)])
        plugin.run(ctx)
        col_names = {c.name for c in ctx["attributes"].columns}
        assert "name" in col_names

    def test_root_table_has_fk_to_attributes(self):
        plugin = AppendOnlyTablePlugin()
        ctx = make_ctx()
        plugin.run(ctx)
        root = ctx["root_table"]
        fk_targets = {fk.column.table.name for fk in root.foreign_keys}
        assert "product_attributes" in fk_targets

    def test_custom_keys(self):
        plugin = AppendOnlyTablePlugin(root_key="r", attributes_key="a")
        ctx = make_ctx()
        plugin.run(ctx)
        assert "r" in ctx
        assert "a" in ctx

    def test_singleton_group_is_table(self):
        assert AppendOnlyTablePlugin.singleton_group == "__table__"


class TestAppendOnlyViewPlugin:
    def _ctx_with_tables(
        self, root_key="root_table", attributes_key="attributes"
    ):
        plugin = AppendOnlyTablePlugin(
            root_key=root_key, attributes_key=attributes_key
        )
        ctx = make_ctx()
        plugin.run(ctx)
        return ctx

    def test_view_registered_in_metadata(self):
        plugin = AppendOnlyViewPlugin()
        ctx = self._ctx_with_tables()
        plugin.run(ctx)
        views = ctx.metadata.info.get("views")
        assert views is not None

    def test_view_in_dim_schema(self):
        plugin = AppendOnlyViewPlugin()
        ctx = self._ctx_with_tables()
        plugin.run(ctx)
        views = ctx.metadata.info["views"].views
        assert any(v.schema == "dim" and v.name == "product" for v in views)

    def test_proxy_stored_under_primary_key(self):
        plugin = AppendOnlyViewPlugin()
        ctx = self._ctx_with_tables()
        plugin.run(ctx)
        assert isinstance(ctx["primary"], Table)

    def test_proxy_has_user_columns(self):
        plugin = AppendOnlyViewPlugin()
        ctx = self._ctx_with_tables()
        plugin.run(ctx)
        proxy = ctx["primary"]
        assert "name" in {c.name for c in proxy.columns}

    def test_view_definition_references_both_tables(self):
        plugin = AppendOnlyViewPlugin()
        ctx = self._ctx_with_tables()
        plugin.run(ctx)
        views = ctx.metadata.info["views"].views
        view = next(v for v in views if v.schema == "dim")
        assert "product_root" in view.definition
        assert "product_attributes" in view.definition

    def test_custom_keys(self):
        table_plugin = AppendOnlyTablePlugin(root_key="r", attributes_key="a")
        ctx = make_ctx()
        table_plugin.run(ctx)
        view_plugin = AppendOnlyViewPlugin(
            root_key="r", attributes_key="a", primary_key="p"
        )
        view_plugin.run(ctx)
        assert "p" in ctx


class TestAppendOnlyTriggerPlugin:
    def _ctx_with_tables_and_view(self, view_key="api"):
        table_plugin = AppendOnlyTablePlugin()
        ctx = make_ctx()
        table_plugin.run(ctx)
        AppendOnlyViewPlugin().run(ctx)
        ctx[view_key] = make_view("product", "api")
        return ctx

    def test_registers_functions_for_primary_view(self):
        plugin = append_only_trigger_plugin()
        ctx = self._ctx_with_tables_and_view()
        plugin.run(ctx)
        functions = ctx.metadata.info.get("functions")
        assert functions is not None
        # dim view + api view = 6 functions total
        assert len(functions.functions) == 6

    def test_registers_triggers_for_primary_view(self):
        plugin = append_only_trigger_plugin()
        ctx = self._ctx_with_tables_and_view()
        plugin.run(ctx)
        assert len(ctx.metadata.info["triggers"].triggers) == 6

    def test_skips_api_view_when_key_absent(self):
        plugin = append_only_trigger_plugin(view_key="nonexistent")
        table_plugin = AppendOnlyTablePlugin()
        ctx = make_ctx()
        table_plugin.run(ctx)
        AppendOnlyViewPlugin().run(ctx)
        plugin.run(ctx)
        # Only the dim view gets 3 functions (api view skipped)
        assert len(ctx.metadata.info["functions"].functions) == 3
