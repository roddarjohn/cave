"""Unit tests for TableCheckPlugin and TriggerCheckPlugin."""

import pytest
from sqlalchemy import Column, Integer, String, Table

from cave.check import CaveCheck
from cave.errors import CaveValidationError
from cave.plugins.check import (
    TableCheckPlugin,
    TriggerCheckPlugin,
)
from tests.unit.plugins.conftest import make_ctx


class TestTableCheckPlugin:
    def test_appends_check_constraint_to_table(self):
        from cave.plugins.simple import SimpleTablePlugin

        ctx = make_ctx(
            schema_items=[
                Column("price", Integer),
                CaveCheck("{price} > 0", name="pos_price"),
            ]
        )
        SimpleTablePlugin().run(ctx)
        TableCheckPlugin().run(ctx)
        table = ctx["primary"]
        ck_names = [c.name for c in table.constraints if hasattr(c, "sqltext")]
        assert "pos_price" in ck_names

    def test_multi_column_constraint(self):
        from cave.plugins.simple import SimpleTablePlugin

        ctx = make_ctx(
            schema_items=[
                Column("price", Integer),
                Column("qty", Integer),
                CaveCheck(
                    "{price} * {qty} <= 1000000",
                    name="max_total",
                ),
            ]
        )
        SimpleTablePlugin().run(ctx)
        TableCheckPlugin().run(ctx)
        table = ctx["primary"]
        ck_names = [c.name for c in table.constraints if hasattr(c, "sqltext")]
        assert "max_total" in ck_names

    def test_no_checks_is_noop(self):
        from cave.plugins.simple import SimpleTablePlugin

        ctx = make_ctx(schema_items=[Column("name", String)])
        SimpleTablePlugin().run(ctx)
        TableCheckPlugin().run(ctx)
        # No error raised, table still valid.
        assert isinstance(ctx["primary"], Table)

    def test_unknown_column_raises(self):
        from cave.plugins.simple import SimpleTablePlugin

        ctx = make_ctx(
            schema_items=[
                Column("price", Integer),
                CaveCheck("{nonexistent} > 0", name="bad"),
            ]
        )
        SimpleTablePlugin().run(ctx)
        with pytest.raises(CaveValidationError, match="nonexistent"):
            TableCheckPlugin().run(ctx)

    def test_custom_table_key(self):
        from cave.plugins.simple import SimpleTablePlugin

        ctx = make_ctx(
            schema_items=[
                Column("price", Integer),
                CaveCheck("{price} > 0", name="pos"),
            ]
        )
        SimpleTablePlugin(table_key="my_table").run(ctx)
        TableCheckPlugin(table_key="my_table").run(ctx)
        table = ctx["my_table"]
        ck_names = [c.name for c in table.constraints if hasattr(c, "sqltext")]
        assert "pos" in ck_names

    def test_requires_dynamic_table_key(self):
        plugin = TableCheckPlugin()
        assert "primary" in plugin.resolved_requires()

    def test_multiple_checks(self):
        from cave.plugins.simple import SimpleTablePlugin

        ctx = make_ctx(
            schema_items=[
                Column("price", Integer),
                Column("qty", Integer),
                CaveCheck("{price} > 0", name="pos_price"),
                CaveCheck("{qty} >= 0", name="nonneg_qty"),
            ]
        )
        SimpleTablePlugin().run(ctx)
        TableCheckPlugin().run(ctx)
        table = ctx["primary"]
        ck_names = [c.name for c in table.constraints if hasattr(c, "sqltext")]
        assert "pos_price" in ck_names
        assert "nonneg_qty" in ck_names


class TestTriggerCheckPlugin:
    def _ctx_with_eav(
        self,
        schema_items=None,
    ):
        from cave.plugins.eav import (
            EAVTablePlugin,
            EAVViewPlugin,
        )

        if schema_items is None:
            schema_items = [
                Column("price", Integer),
                CaveCheck("{price} > 0", name="pos_price"),
            ]
        ctx = make_ctx(schema_items=schema_items)
        EAVTablePlugin().run(ctx)
        EAVViewPlugin().run(ctx)
        return ctx

    def test_registers_functions(self):
        ctx = self._ctx_with_eav()
        TriggerCheckPlugin(view_key="primary").run(ctx)
        functions = ctx.metadata.info.get("functions")
        assert functions is not None
        # 2 ops (insert, update) on the primary view
        assert len(functions.functions) == 2

    def test_registers_triggers(self):
        ctx = self._ctx_with_eav()
        TriggerCheckPlugin(view_key="primary").run(ctx)
        triggers = ctx.metadata.info.get("triggers")
        assert triggers is not None
        assert len(triggers.triggers) == 2

    def test_trigger_names_have_check_prefix(self):
        ctx = self._ctx_with_eav()
        TriggerCheckPlugin(view_key="primary").run(ctx)
        triggers = ctx.metadata.info["triggers"].triggers
        for t in triggers:
            assert t.name.startswith("_check_")

    def test_function_body_contains_new_prefix(self):
        ctx = self._ctx_with_eav()
        TriggerCheckPlugin(view_key="primary").run(ctx)
        fns = ctx.metadata.info["functions"].functions
        insert_fn = next(f for f in fns if "insert" in f.name)
        assert "NEW.price" in insert_fn.definition

    def test_function_body_contains_check_name(self):
        ctx = self._ctx_with_eav()
        TriggerCheckPlugin(view_key="primary").run(ctx)
        fns = ctx.metadata.info["functions"].functions
        insert_fn = next(f for f in fns if "insert" in f.name)
        assert "pos_price" in insert_fn.definition

    def test_no_checks_is_noop(self):
        ctx = self._ctx_with_eav(schema_items=[Column("price", Integer)])
        TriggerCheckPlugin(view_key="primary").run(ctx)
        assert ctx.metadata.info.get("functions") is None

    def test_unknown_column_raises(self):
        ctx = self._ctx_with_eav(
            schema_items=[
                Column("price", Integer),
                CaveCheck("{bogus} > 0", name="bad"),
            ]
        )
        with pytest.raises(CaveValidationError, match="bogus"):
            TriggerCheckPlugin(view_key="primary").run(ctx)

    def test_skips_absent_view_key(self):
        ctx = self._ctx_with_eav()
        TriggerCheckPlugin(view_key="nonexistent").run(ctx)
        # No triggers registered since view_key not in ctx.
        assert ctx.metadata.info.get("triggers") is None

    def test_multi_column_check(self):
        ctx = self._ctx_with_eav(
            schema_items=[
                Column("price", Integer),
                Column("qty", Integer),
                CaveCheck(
                    "{price} * {qty} <= 1000000",
                    name="max_total",
                ),
            ]
        )
        TriggerCheckPlugin(view_key="primary").run(ctx)
        fns = ctx.metadata.info["functions"].functions
        insert_fn = next(f for f in fns if "insert" in f.name)
        assert "NEW.price" in insert_fn.definition
        assert "NEW.qty" in insert_fn.definition

    def test_only_insert_and_update_ops(self):
        """No DELETE trigger — there's no NEW row to check."""
        ctx = self._ctx_with_eav()
        TriggerCheckPlugin(view_key="primary").run(ctx)
        triggers = ctx.metadata.info["triggers"].triggers
        ops = {t.name.split("_")[-1] for t in triggers}
        assert "insert" in ops
        assert "update" in ops
        assert "delete" not in ops

    def test_ordering_valid_with_later_eav_triggers(self):
        """Check triggers (_check_) sort before EAV triggers (dim_)."""
        from cave.plugins.eav import EAVTriggerPlugin

        ctx = self._ctx_with_eav()
        # Register check triggers first (_check_...)
        TriggerCheckPlugin(view_key="primary").run(ctx)
        # Then register EAV triggers (dim_product_...)
        EAVTriggerPlugin(view_key="nonexistent").run(ctx)
        # No error — _check_ sorts before dim_

    def test_ordering_invalid_raises(self):
        """Check triggers that sort after existing triggers raise."""
        from sqlalchemy_declarative_extensions import register_trigger
        from sqlalchemy_declarative_extensions.dialects.postgresql import (
            Trigger,
        )

        ctx = self._ctx_with_eav()
        view_fullname = f"{ctx.schemaname}.{ctx.tablename}"
        # Pre-register a trigger that sorts BEFORE _check_
        register_trigger(
            ctx.metadata,
            Trigger.instead_of(
                "insert",
                on=view_fullname,
                execute=f"{ctx.schemaname}._aaa_product_insert",
                name="_aaa_product_insert",
            ).for_each_row(),
        )
        # Now the check plugin registers _check_dim_product_insert
        # which sorts AFTER _aaa_, so validation should raise.
        with pytest.raises(CaveValidationError, match="alphabetical"):
            TriggerCheckPlugin(view_key="primary").run(ctx)
