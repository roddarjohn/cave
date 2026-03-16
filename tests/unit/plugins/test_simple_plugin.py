"""Unit tests for SimpleTablePlugin and InsteadOfTriggerPlugin."""

from sqlalchemy import (
    Column,
    Computed,
    Integer,
    String,
    Table,
    UniqueConstraint,
)

from pgcraft.check import PGCraftCheck
from pgcraft.plugins.simple import (
    _NAMING_DEFAULTS,
    SimpleTablePlugin,
    _build_simple_ops_with_columns,
)
from pgcraft.plugins.trigger import InsteadOfTriggerPlugin
from tests.unit.plugins.conftest import make_ctx, make_view


class TestSimpleTablePlugin:
    def test_creates_table_in_metadata(self):
        plugin = SimpleTablePlugin()
        ctx = make_ctx()
        plugin.run(ctx)
        assert "dim.product" in ctx.metadata.tables

    def test_table_stored_under_default_key(self):
        plugin = SimpleTablePlugin()
        ctx = make_ctx()
        plugin.run(ctx)
        assert isinstance(ctx["primary"], Table)

    def test_custom_table_key(self):
        plugin = SimpleTablePlugin(table_key="my_table")
        ctx = make_ctx()
        plugin.run(ctx)
        assert "my_table" in ctx

    def test_table_includes_pk_columns(self):
        plugin = SimpleTablePlugin()
        ctx = make_ctx()
        plugin.run(ctx)
        table = ctx["primary"]
        pk_col_names = {c.name for c in table.columns if c.primary_key}
        assert "id" in pk_col_names

    def test_table_includes_dimensions(self):
        plugin = SimpleTablePlugin()
        ctx = make_ctx(
            schema_items=[
                Column("name", String),
                Column("code", String),
            ]
        )
        plugin.run(ctx)
        table = ctx["primary"]
        col_names = {c.name for c in table.columns}
        assert "name" in col_names
        assert "code" in col_names

    def test_table_has_correct_schema(self):
        plugin = SimpleTablePlugin()
        ctx = make_ctx(schemaname="myschema")
        plugin.run(ctx)
        table = ctx["primary"]
        assert table.schema == "myschema"

    def test_singleton_group_is_table(self):
        assert SimpleTablePlugin.singleton_group == "__table__"

    def test_requires_pk_columns(self):
        plugin = SimpleTablePlugin()
        assert "pk_columns" in plugin.resolved_requires()

    def test_table_includes_unique_constraint(self):
        plugin = SimpleTablePlugin()
        ctx = make_ctx(
            schema_items=[
                Column("code", String),
                UniqueConstraint("code"),
            ]
        )
        plugin.run(ctx)
        table = ctx["primary"]
        uq_constraints = [
            c for c in table.constraints if isinstance(c, UniqueConstraint)
        ]
        assert len(uq_constraints) == 1

    def test_table_includes_computed_column(self):
        plugin = SimpleTablePlugin()
        ctx = make_ctx(
            schema_items=[
                Column("price", Integer),
                Column("qty", Integer),
                Column(
                    "total",
                    Integer,
                    Computed("price * qty"),
                ),
            ]
        )
        plugin.run(ctx)
        table = ctx["primary"]
        col_names = {c.name for c in table.columns}
        assert "total" in col_names
        assert table.c["total"].computed is not None

    def test_cave_check_excluded_from_table(self):
        plugin = SimpleTablePlugin()
        ctx = make_ctx(
            schema_items=[
                Column("price", Integer),
                PGCraftCheck("{price} > 0", name="pos"),
            ]
        )
        plugin.run(ctx)
        table = ctx["primary"]
        col_names = {c.name for c in table.columns}
        assert "price" in col_names
        # PGCraftCheck should not appear as a column or constraint
        ck_names = [c.name for c in table.constraints if hasattr(c, "sqltext")]
        assert "pos" not in ck_names


class TestSimpleTriggerPlugin:
    def _ctx_with_table_and_view(self, table_key="primary", view_key="api"):
        plugin = SimpleTablePlugin(table_key=table_key)
        ctx = make_ctx()
        plugin.run(ctx)
        ctx[view_key] = make_view("product", "api")
        return ctx

    def test_registers_functions(self):
        plugin = InsteadOfTriggerPlugin(
            ops_builder=_build_simple_ops_with_columns(None, "primary"),
            naming_defaults=_NAMING_DEFAULTS,
            function_key="simple_function",
            trigger_key="simple_trigger",
            view_key="api",
            include_private_view=False,
        )
        ctx = self._ctx_with_table_and_view()
        plugin.run(ctx)
        functions = ctx.metadata.info.get("functions")
        assert functions is not None
        assert len(functions.functions) == 3

    def test_registers_triggers(self):
        plugin = InsteadOfTriggerPlugin(
            ops_builder=_build_simple_ops_with_columns(None, "primary"),
            naming_defaults=_NAMING_DEFAULTS,
            function_key="simple_function",
            trigger_key="simple_trigger",
            view_key="api",
            include_private_view=False,
        )
        ctx = self._ctx_with_table_and_view()
        plugin.run(ctx)
        triggers = ctx.metadata.info.get("triggers")
        assert triggers is not None
        assert len(triggers.triggers) == 3

    def test_custom_table_key_and_view_key(self):
        plugin = InsteadOfTriggerPlugin(
            ops_builder=_build_simple_ops_with_columns(None, "t"),
            naming_defaults=_NAMING_DEFAULTS,
            function_key="simple_function",
            trigger_key="simple_trigger",
            view_key="v",
            include_private_view=False,
        )
        ctx = self._ctx_with_table_and_view(table_key="t", view_key="v")
        plugin.run(ctx)
        assert len(ctx.metadata.info["functions"].functions) == 3

    def test_missing_view_key_skips(self):
        plugin = InsteadOfTriggerPlugin(
            ops_builder=_build_simple_ops_with_columns(None, "primary"),
            naming_defaults=_NAMING_DEFAULTS,
            function_key="simple_function",
            trigger_key="simple_trigger",
            view_key="nonexistent",
            include_private_view=False,
        )
        ctx = make_ctx()
        SimpleTablePlugin().run(ctx)
        plugin.run(ctx)
        # No view → no triggers registered
        assert ctx.metadata.info.get("triggers") is None
