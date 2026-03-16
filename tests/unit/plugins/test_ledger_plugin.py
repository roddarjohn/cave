"""Unit tests for ledger plugins."""

import pytest
from sqlalchemy import Column, Integer, Numeric, String, Table
from sqlalchemy.dialects.postgresql import UUID

from pgcraft.errors import PGCraftValidationError
from pgcraft.plugins.entry_id import UUIDEntryIDPlugin
from pgcraft.plugins.ledger import (
    _NAMING_DEFAULTS,
    DoubleEntryPlugin,
    DoubleEntryTriggerPlugin,
    LedgerBalanceCheckPlugin,
    LedgerBalanceViewPlugin,
    LedgerLatestViewPlugin,
    LedgerTablePlugin,
    _make_ledger_ops_builder,
)
from pgcraft.plugins.trigger import InsteadOfTriggerPlugin
from tests.unit.plugins.conftest import make_ctx, make_view


def _ledger_ctx(**kwargs):
    """Return a FactoryContext with entry_id_column pre-populated."""
    ctx = make_ctx(**kwargs)
    UUIDEntryIDPlugin().run(ctx)
    return ctx


class TestLedgerTablePlugin:
    def test_creates_table_in_metadata(self):
        plugin = LedgerTablePlugin()
        ctx = _ledger_ctx()
        plugin.run(ctx)
        assert "dim.product" in ctx.metadata.tables

    def test_table_stored_under_default_key(self):
        plugin = LedgerTablePlugin()
        ctx = _ledger_ctx()
        plugin.run(ctx)
        assert isinstance(ctx["primary"], Table)

    def test_custom_table_key(self):
        plugin = LedgerTablePlugin(table_key="my_table")
        ctx = _ledger_ctx()
        plugin.run(ctx)
        assert "my_table" in ctx

    def test_table_includes_pk_columns(self):
        plugin = LedgerTablePlugin()
        ctx = _ledger_ctx()
        plugin.run(ctx)
        table = ctx["primary"]
        pk_col_names = {c.name for c in table.columns if c.primary_key}
        assert "id" in pk_col_names

    def test_table_includes_entry_id(self):
        plugin = LedgerTablePlugin()
        ctx = _ledger_ctx()
        plugin.run(ctx)
        table = ctx["primary"]
        assert "entry_id" in {c.name for c in table.columns}
        assert isinstance(table.c["entry_id"].type, UUID)
        assert not table.c["entry_id"].nullable

    def test_table_includes_value_column_integer(self):
        plugin = LedgerTablePlugin()
        ctx = _ledger_ctx()
        plugin.run(ctx)
        table = ctx["primary"]
        assert "value" in {c.name for c in table.columns}
        assert isinstance(table.c["value"].type, Integer)
        assert not table.c["value"].nullable

    def test_table_includes_value_column_numeric(self):
        plugin = LedgerTablePlugin(value_type="numeric")
        ctx = _ledger_ctx()
        plugin.run(ctx)
        table = ctx["primary"]
        assert isinstance(table.c["value"].type, Numeric)
        assert not table.c["value"].nullable

    def test_table_includes_created_at(self):
        plugin = LedgerTablePlugin()
        ctx = _ledger_ctx()
        plugin.run(ctx)
        table = ctx["primary"]
        assert "created_at" in {c.name for c in table.columns}

    def test_table_includes_dimensions(self):
        plugin = LedgerTablePlugin()
        ctx = _ledger_ctx(
            schema_items=[
                Column("category", String),
                Column("region", String),
            ]
        )
        plugin.run(ctx)
        table = ctx["primary"]
        col_names = {c.name for c in table.columns}
        assert "category" in col_names
        assert "region" in col_names

    def test_table_has_correct_schema(self):
        plugin = LedgerTablePlugin()
        ctx = _ledger_ctx(schemaname="myschema")
        plugin.run(ctx)
        table = ctx["primary"]
        assert table.schema == "myschema"

    def test_singleton_group_is_table(self):
        assert LedgerTablePlugin.singleton_group == "__table__"

    def test_requires_pk_entry_id_and_created_at(self):
        plugin = LedgerTablePlugin()
        reqs = plugin.resolved_requires()
        assert "pk_columns" in reqs
        assert "entry_id_column" in reqs
        assert "created_at_column" in reqs

    def test_invalid_value_type_raises(self):
        with pytest.raises(PGCraftValidationError, match="float"):
            LedgerTablePlugin(value_type="float")

    def test_sets_root(self):
        plugin = LedgerTablePlugin()
        ctx = _ledger_ctx()
        plugin.run(ctx)
        assert "__root__" in ctx


class TestLedgerTriggerPlugin:
    def _ctx_with_table_and_view(self, table_key="primary", view_key="api"):
        ctx = _ledger_ctx()
        LedgerTablePlugin(table_key=table_key).run(ctx)
        ctx[view_key] = make_view("product", "api")
        return ctx

    def _make_plugin(self, table_key="primary", view_key="api"):
        return InsteadOfTriggerPlugin(
            ops_builder=_make_ledger_ops_builder(table_key, view_key),
            naming_defaults=_NAMING_DEFAULTS,
            function_key="ledger_function",
            trigger_key="ledger_trigger",
            view_key=view_key,
            include_private_view=False,
            extra_requires=[table_key, "entry_id_column"],
        )

    def test_registers_three_functions(self):
        plugin = self._make_plugin()
        ctx = self._ctx_with_table_and_view()
        plugin.run(ctx)
        functions = ctx.metadata.info.get("functions")
        assert functions is not None
        assert len(functions.functions) == 3

    def test_registers_three_triggers(self):
        plugin = self._make_plugin()
        ctx = self._ctx_with_table_and_view()
        plugin.run(ctx)
        triggers = ctx.metadata.info.get("triggers")
        assert triggers is not None
        assert len(triggers.triggers) == 3

    def test_custom_table_key_and_view_key(self):
        plugin = self._make_plugin(table_key="t", view_key="v")
        ctx = self._ctx_with_table_and_view(table_key="t", view_key="v")
        plugin.run(ctx)
        assert len(ctx.metadata.info["functions"].functions) == 3

    def test_missing_view_key_no_triggers(self):
        plugin = self._make_plugin(view_key="nonexistent")
        ctx = _ledger_ctx()
        LedgerTablePlugin().run(ctx)
        plugin.run(ctx)
        triggers = ctx.metadata.info.get("triggers")
        assert triggers is None


class TestLedgerBalanceViewPlugin:
    def _ctx_with_table(self, **kwargs):
        ctx = _ledger_ctx(**kwargs)
        LedgerTablePlugin().run(ctx)
        return ctx

    def test_registers_balance_view(self):
        ctx = self._ctx_with_table(schema_items=[Column("category", String)])
        plugin = LedgerBalanceViewPlugin(dimensions=["category"])
        plugin.run(ctx)
        views = ctx.metadata.info.get("views")
        assert views is not None
        assert len(views.views) == 1

    def test_stores_view_name_in_ctx(self):
        ctx = self._ctx_with_table(schema_items=[Column("category", String)])
        plugin = LedgerBalanceViewPlugin(dimensions=["category"])
        plugin.run(ctx)
        assert "balance_view" in ctx

    def test_custom_balance_view_key(self):
        ctx = self._ctx_with_table(schema_items=[Column("category", String)])
        plugin = LedgerBalanceViewPlugin(
            dimensions=["category"], balance_view_key="my_bv"
        )
        plugin.run(ctx)
        assert "my_bv" in ctx

    def test_multiple_dimensions(self):
        ctx = self._ctx_with_table(
            schema_items=[
                Column("category", String),
                Column("region", String),
            ]
        )
        plugin = LedgerBalanceViewPlugin(dimensions=["category", "region"])
        plugin.run(ctx)
        assert "balance_view" in ctx

    def test_empty_dimensions_raises(self):
        with pytest.raises(PGCraftValidationError, match="non-empty"):
            LedgerBalanceViewPlugin(dimensions=[])

    def test_produces_balance_view_key(self):
        plugin = LedgerBalanceViewPlugin(dimensions=["x"])
        assert "balance_view" in plugin.resolved_produces()

    def test_requires_table_key(self):
        plugin = LedgerBalanceViewPlugin(dimensions=["x"])
        assert "primary" in plugin.resolved_requires()


class TestDoubleEntryPlugin:
    def test_injects_direction_column(self):
        ctx = _ledger_ctx()
        DoubleEntryPlugin().run(ctx)
        injected_names = [c.name for c in ctx.injected_columns]
        assert "direction" in injected_names

    def test_direction_column_is_not_nullable(self):
        ctx = _ledger_ctx()
        DoubleEntryPlugin().run(ctx)
        direction = next(
            c for c in ctx.injected_columns if c.name == "direction"
        )
        assert not direction.nullable

    def test_stores_column_name_in_ctx(self):
        ctx = _ledger_ctx()
        DoubleEntryPlugin().run(ctx)
        assert ctx["double_entry_columns"] == "direction"

    def test_custom_column_name(self):
        ctx = _ledger_ctx()
        DoubleEntryPlugin(column_name="side").run(ctx)
        assert ctx["double_entry_columns"] == "side"
        injected_names = [c.name for c in ctx.injected_columns]
        assert "side" in injected_names

    def test_singleton_group(self):
        assert DoubleEntryPlugin.singleton_group == "__double_entry__"

    def test_table_includes_direction_column(self):
        ctx = _ledger_ctx()
        DoubleEntryPlugin().run(ctx)
        LedgerTablePlugin().run(ctx)
        table = ctx["primary"]
        assert "direction" in {c.name for c in table.columns}

    def test_direction_column_has_check_constraint(self):
        ctx = _ledger_ctx()
        DoubleEntryPlugin().run(ctx)
        LedgerTablePlugin().run(ctx)
        table = ctx["primary"]
        col = table.c["direction"]
        checks = [c for c in col.constraints if hasattr(c, "sqltext")]
        assert len(checks) == 1
        text = str(checks[0].sqltext)
        assert "debit" in text
        assert "credit" in text


class TestDoubleEntryTriggerPlugin:
    def _ctx_with_double_entry_table(self):
        ctx = _ledger_ctx()
        DoubleEntryPlugin().run(ctx)
        LedgerTablePlugin().run(ctx)
        return ctx

    def test_registers_function(self):
        ctx = self._ctx_with_double_entry_table()
        DoubleEntryTriggerPlugin().run(ctx)
        functions = ctx.metadata.info.get("functions")
        assert functions is not None
        assert len(functions.functions) == 1

    def test_registers_trigger(self):
        ctx = self._ctx_with_double_entry_table()
        DoubleEntryTriggerPlugin().run(ctx)
        triggers = ctx.metadata.info.get("triggers")
        assert triggers is not None
        assert len(triggers.triggers) == 1

    def test_trigger_is_statement_level(self):
        ctx = self._ctx_with_double_entry_table()
        DoubleEntryTriggerPlugin().run(ctx)
        trigger = ctx.metadata.info["triggers"].triggers[0]
        assert trigger.for_each.value == "STATEMENT"

    def test_requires_table_and_double_entry(self):
        plugin = DoubleEntryTriggerPlugin()
        reqs = plugin.resolved_requires()
        assert "primary" in reqs
        assert "double_entry_columns" in reqs
        assert "entry_id_column" in reqs


class TestLedgerLatestViewPlugin:
    def _ctx_with_table(self, **kwargs):
        ctx = _ledger_ctx(**kwargs)
        LedgerTablePlugin().run(ctx)
        return ctx

    def test_registers_latest_view(self):
        ctx = self._ctx_with_table(schema_items=[Column("order_id", String)])
        LedgerLatestViewPlugin(dimensions=["order_id"]).run(ctx)
        views = ctx.metadata.info.get("views")
        assert views is not None
        assert len(views.views) == 1

    def test_stores_view_name_in_ctx(self):
        ctx = self._ctx_with_table(schema_items=[Column("order_id", String)])
        LedgerLatestViewPlugin(dimensions=["order_id"]).run(ctx)
        assert "latest_view" in ctx

    def test_custom_latest_view_key(self):
        ctx = self._ctx_with_table(schema_items=[Column("order_id", String)])
        LedgerLatestViewPlugin(
            dimensions=["order_id"],
            latest_view_key="my_latest",
        ).run(ctx)
        assert "my_latest" in ctx

    def test_multiple_dimensions(self):
        ctx = self._ctx_with_table(
            schema_items=[
                Column("entity_type", String),
                Column("entity_id", String),
            ]
        )
        LedgerLatestViewPlugin(dimensions=["entity_type", "entity_id"]).run(ctx)
        assert "latest_view" in ctx

    def test_empty_dimensions_raises(self):
        with pytest.raises(PGCraftValidationError, match="non-empty"):
            LedgerLatestViewPlugin(dimensions=[])

    def test_produces_latest_view_key(self):
        plugin = LedgerLatestViewPlugin(dimensions=["x"])
        assert "latest_view" in plugin.resolved_produces()

    def test_requires_table_key_and_created_at(self):
        plugin = LedgerLatestViewPlugin(dimensions=["x"])
        reqs = plugin.resolved_requires()
        assert "primary" in reqs
        assert "created_at_column" in reqs


class TestLedgerBalanceCheckPlugin:
    def _ctx_with_table(self, **kwargs):
        ctx = _ledger_ctx(**kwargs)
        LedgerTablePlugin().run(ctx)
        return ctx

    def test_registers_function(self):
        ctx = self._ctx_with_table(schema_items=[Column("warehouse", String)])
        LedgerBalanceCheckPlugin(dimensions=["warehouse"]).run(ctx)
        functions = ctx.metadata.info.get("functions")
        assert functions is not None
        assert len(functions.functions) == 1

    def test_registers_trigger(self):
        ctx = self._ctx_with_table(schema_items=[Column("warehouse", String)])
        LedgerBalanceCheckPlugin(dimensions=["warehouse"]).run(ctx)
        triggers = ctx.metadata.info.get("triggers")
        assert triggers is not None
        assert len(triggers.triggers) == 1

    def test_trigger_is_statement_level(self):
        ctx = self._ctx_with_table(schema_items=[Column("warehouse", String)])
        LedgerBalanceCheckPlugin(dimensions=["warehouse"]).run(ctx)
        trigger = ctx.metadata.info["triggers"].triggers[0]
        assert trigger.for_each.value == "STATEMENT"

    def test_custom_min_balance(self):
        ctx = self._ctx_with_table(schema_items=[Column("warehouse", String)])
        plugin = LedgerBalanceCheckPlugin(
            dimensions=["warehouse"], min_balance=-100
        )
        plugin.run(ctx)
        assert len(ctx.metadata.info["functions"].functions) == 1

    def test_empty_dimensions_raises(self):
        with pytest.raises(PGCraftValidationError, match="non-empty"):
            LedgerBalanceCheckPlugin(dimensions=[])

    def test_multiple_dimensions(self):
        ctx = self._ctx_with_table(
            schema_items=[
                Column("warehouse", String),
                Column("sku", String),
            ]
        )
        LedgerBalanceCheckPlugin(dimensions=["warehouse", "sku"]).run(ctx)
        assert len(ctx.metadata.info["triggers"].triggers) == 1

    def test_requires_table_key(self):
        plugin = LedgerBalanceCheckPlugin(dimensions=["x"])
        assert "primary" in plugin.resolved_requires()
