"""Unit tests for EAV{Table,View,Trigger}Plugin in isolation."""

from sqlalchemy import Boolean, Column, Integer, String, Table

from pgcraft.plugins.eav import (
    _NAMING_DEFAULTS,
    EAVTablePlugin,
    EAVViewPlugin,
    _make_eav_ops_builder,
)
from pgcraft.plugins.trigger import InsteadOfTriggerPlugin
from tests.unit.plugins.conftest import make_ctx, make_view


class TestEAVTablePlugin:
    def test_entity_table_stored_under_default_key(self):
        plugin = EAVTablePlugin()
        ctx = make_ctx()
        plugin.run(ctx)
        assert isinstance(ctx["entity"], Table)

    def test_attribute_table_stored_under_default_key(self):
        plugin = EAVTablePlugin()
        ctx = make_ctx()
        plugin.run(ctx)
        assert isinstance(ctx["attribute"], Table)

    def test_mappings_stored_under_default_key(self):
        plugin = EAVTablePlugin()
        ctx = make_ctx()
        plugin.run(ctx)
        mappings = ctx["eav_mappings"]
        assert isinstance(mappings, list)
        assert len(mappings) == 1  # one dimension column ("name")

    def test_entity_table_name(self):
        plugin = EAVTablePlugin()
        ctx = make_ctx()
        plugin.run(ctx)
        assert ctx["entity"].name == "product_entity"

    def test_attribute_table_name(self):
        plugin = EAVTablePlugin()
        ctx = make_ctx()
        plugin.run(ctx)
        assert ctx["attribute"].name == "product_attribute"

    def test_entity_table_has_pk(self):
        plugin = EAVTablePlugin()
        ctx = make_ctx()
        plugin.run(ctx)
        entity = ctx["entity"]
        assert any(c.primary_key for c in entity.columns)

    def test_attribute_table_has_entity_fk(self):
        plugin = EAVTablePlugin()
        ctx = make_ctx()
        plugin.run(ctx)
        attr = ctx["attribute"]
        fk_targets = {fk.column.table.name for fk in attr.foreign_keys}
        assert "product_entity" in fk_targets

    def test_attribute_table_has_value_column_for_string(self):
        plugin = EAVTablePlugin()
        ctx = make_ctx(schema_items=[Column("label", String)])
        plugin.run(ctx)
        attr = ctx["attribute"]
        assert "string_value" in {c.name for c in attr.columns}

    def test_attribute_table_has_value_column_for_integer(self):
        plugin = EAVTablePlugin()
        ctx = make_ctx(schema_items=[Column("count", Integer)])
        plugin.run(ctx)
        attr = ctx["attribute"]
        assert "integer_value" in {c.name for c in attr.columns}

    def test_mapping_nullable_defaults_true(self):
        plugin = EAVTablePlugin()
        ctx = make_ctx(schema_items=[Column("name", String)])
        plugin.run(ctx)
        mapping = ctx["eav_mappings"][0]
        assert mapping.nullable is True

    def test_mapping_nullable_false_when_column_not_nullable(self):
        plugin = EAVTablePlugin()
        ctx = make_ctx(schema_items=[Column("name", String, nullable=False)])
        plugin.run(ctx)
        mapping = ctx["eav_mappings"][0]
        assert mapping.nullable is False

    def test_custom_keys(self):
        plugin = EAVTablePlugin(
            entity_key="e", attribute_key="a", mappings_key="m"
        )
        ctx = make_ctx()
        plugin.run(ctx)
        assert "e" in ctx
        assert "a" in ctx
        assert "m" in ctx

    def test_singleton_group_is_table(self):
        assert EAVTablePlugin.singleton_group == "__table__"


class TestEAVViewPlugin:
    def _ctx_with_tables(self):
        plugin = EAVTablePlugin()
        ctx = make_ctx()
        plugin.run(ctx)
        return ctx

    def test_pivot_view_registered(self):
        plugin = EAVViewPlugin()
        ctx = self._ctx_with_tables()
        plugin.run(ctx)
        views = ctx.metadata.info.get("views")
        assert views is not None

    def test_pivot_view_in_dim_schema(self):
        plugin = EAVViewPlugin()
        ctx = self._ctx_with_tables()
        plugin.run(ctx)
        views = ctx.metadata.info["views"].views
        assert any(v.schema == "dim" and v.name == "product" for v in views)

    def test_proxy_stored_under_primary_key(self):
        plugin = EAVViewPlugin()
        ctx = self._ctx_with_tables()
        plugin.run(ctx)
        assert isinstance(ctx["primary"], Table)

    def test_proxy_has_mapped_columns(self):
        plugin = EAVViewPlugin()
        ctx = make_ctx(
            schema_items=[Column("name", String), Column("age", Integer)]
        )
        EAVTablePlugin().run(ctx)
        plugin.run(ctx)
        proxy = ctx["primary"]
        col_names = {c.name for c in proxy.columns}
        assert "name" in col_names
        assert "age" in col_names

    def test_boolean_column_appears_in_proxy(self):
        plugin = EAVViewPlugin()
        ctx = make_ctx(schema_items=[Column("active", Boolean)])
        EAVTablePlugin().run(ctx)
        plugin.run(ctx)
        proxy = ctx["primary"]
        assert "active" in {c.name for c in proxy.columns}

    def test_pivot_view_definition_references_entity_table(self):
        plugin = EAVViewPlugin()
        ctx = self._ctx_with_tables()
        plugin.run(ctx)
        views = ctx.metadata.info["views"].views
        view = next(v for v in views if v.schema == "dim")
        assert "product_entity" in view.definition

    def test_custom_keys(self):
        table_plugin = EAVTablePlugin(
            entity_key="e", attribute_key="a", mappings_key="m"
        )
        ctx = make_ctx()
        table_plugin.run(ctx)
        view_plugin = EAVViewPlugin(
            entity_key="e", attribute_key="a", mappings_key="m", primary_key="p"
        )
        view_plugin.run(ctx)
        assert "p" in ctx


class TestEAVTriggerPlugin:
    def _ctx_with_tables_and_view(self, view_key: str = "api"):
        ctx = make_ctx()
        EAVTablePlugin().run(ctx)
        EAVViewPlugin().run(ctx)
        ctx[view_key] = make_view("product", "api")
        return ctx

    def test_registers_functions_for_both_views(self):
        plugin = InsteadOfTriggerPlugin(
            ops_builder=_make_eav_ops_builder(
                "entity", "attribute", "eav_mappings"
            ),
            naming_defaults=_NAMING_DEFAULTS,
            function_key="eav_function",
            trigger_key="eav_trigger",
            view_key="api",
            extra_requires=["entity", "attribute", "eav_mappings"],
        )
        ctx = self._ctx_with_tables_and_view()
        plugin.run(ctx)
        functions = ctx.metadata.info.get("functions")
        assert functions is not None
        assert len(functions.functions) == 6  # dim + api views × 3 ops

    def test_registers_triggers_for_both_views(self):
        plugin = InsteadOfTriggerPlugin(
            ops_builder=_make_eav_ops_builder(
                "entity", "attribute", "eav_mappings"
            ),
            naming_defaults=_NAMING_DEFAULTS,
            function_key="eav_function",
            trigger_key="eav_trigger",
            view_key="api",
            extra_requires=["entity", "attribute", "eav_mappings"],
        )
        ctx = self._ctx_with_tables_and_view()
        plugin.run(ctx)
        assert len(ctx.metadata.info["triggers"].triggers) == 6

    def test_skips_api_view_when_key_absent(self):
        plugin = InsteadOfTriggerPlugin(
            ops_builder=_make_eav_ops_builder(
                "entity", "attribute", "eav_mappings"
            ),
            naming_defaults=_NAMING_DEFAULTS,
            function_key="eav_function",
            trigger_key="eav_trigger",
            view_key="nonexistent",
            extra_requires=["entity", "attribute", "eav_mappings"],
        )
        ctx = make_ctx()
        EAVTablePlugin().run(ctx)
        EAVViewPlugin().run(ctx)
        plugin.run(ctx)
        assert len(ctx.metadata.info["functions"].functions) == 3

    def test_nullable_false_mapping_rendered_in_function(self):
        """Insert function body should raise for non-nullable attributes."""
        plugin = InsteadOfTriggerPlugin(
            ops_builder=_make_eav_ops_builder(
                "entity", "attribute", "eav_mappings"
            ),
            naming_defaults=_NAMING_DEFAULTS,
            function_key="eav_function",
            trigger_key="eav_trigger",
            view_key="api",
            extra_requires=["entity", "attribute", "eav_mappings"],
        )
        ctx = make_ctx(schema_items=[Column("sku", String, nullable=False)])
        EAVTablePlugin().run(ctx)
        EAVViewPlugin().run(ctx)
        ctx["api"] = make_view("product", "api")
        plugin.run(ctx)
        fns = ctx.metadata.info["functions"].functions
        insert_fn = next(
            f for f in fns if "insert" in f.name and "dim" in f.name
        )
        assert "RAISE EXCEPTION" in insert_fn.definition

    def test_custom_keys(self):
        table_plugin = EAVTablePlugin(
            entity_key="e", attribute_key="a", mappings_key="m"
        )
        ctx = make_ctx()
        table_plugin.run(ctx)
        EAVViewPlugin(entity_key="e", attribute_key="a", mappings_key="m").run(
            ctx
        )
        ctx["api"] = make_view("product", "api")
        trigger_plugin = InsteadOfTriggerPlugin(
            ops_builder=_make_eav_ops_builder("e", "a", "m"),
            naming_defaults=_NAMING_DEFAULTS,
            function_key="eav_function",
            trigger_key="eav_trigger",
            view_key="api",
            extra_requires=["e", "a", "m"],
        )
        trigger_plugin.run(ctx)
        assert len(ctx.metadata.info["functions"].functions) == 6
