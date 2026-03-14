"""Unit tests for LedgerEvent, ParamCollector, and ledger_balances."""

from __future__ import annotations

import pytest
from sqlalchemy import Column, Integer, String, select

from pgcraft.errors import PGCraftValidationError
from pgcraft.ledger.events import (
    LedgerEvent,
    ParamCollector,
    ledger_balances,
)
from pgcraft.plugins.entry_id import UUIDEntryIDPlugin
from pgcraft.plugins.ledger import LedgerTablePlugin
from pgcraft.plugins.ledger_actions import (
    LedgerActionsPlugin,
    _validate_events,
)
from tests.unit.plugins.conftest import make_ctx


def _make_ledger_root(
    schema_items: list | None = None,
) -> tuple:
    """Build a minimal ledger root table for testing."""
    ctx = make_ctx(
        tablename="inventory",
        schemaname="ops",
        schema_items=schema_items
        or [
            Column("sku", String),
            Column("warehouse", String),
            Column("reason", String, nullable=True),
        ],
    )
    UUIDEntryIDPlugin().run(ctx)
    LedgerTablePlugin().run(ctx)
    return ctx["__root__"], ctx


class TestParamCollector:
    def test_returns_literal_column(self):
        p = ParamCollector()
        result = p("warehouse", String)
        assert str(result) == "p_warehouse"

    def test_collects_params(self):
        p = ParamCollector()
        p("warehouse", String)
        p("sku", String)
        params = p.function_params
        assert len(params) == 2
        assert params[0].name == "p_warehouse"
        assert params[1].name == "p_sku"

    def test_accepts_type_instance(self):
        p = ParamCollector()
        p("qty", Integer())
        params = p.function_params
        assert len(params) == 1
        assert params[0].name == "p_qty"


class TestLedgerEventValidation:
    def test_duplicate_names_raise(self):
        root, _ = _make_ledger_root()
        e1 = LedgerEvent(
            name="sync",
            input=lambda p: select(p("x", String).label("x")),
        )
        e2 = LedgerEvent(
            name="sync",
            input=lambda p: select(p("x", String).label("x")),
        )
        with pytest.raises(PGCraftValidationError, match="Duplicate"):
            _validate_events([e1, e2], root)

    def test_diff_keys_required_with_desired(self):
        root, _ = _make_ledger_root()
        event = LedgerEvent(
            name="rec",
            input=lambda p: select(p("sku", String).label("sku")),
            desired=lambda pginput: select(pginput.c.sku),
        )
        with pytest.raises(PGCraftValidationError, match="diff_keys"):
            _validate_events([event], root)

    def test_existing_requires_desired(self):
        root, _ = _make_ledger_root()
        event = LedgerEvent(
            name="rec",
            input=lambda p: select(p("sku", String).label("sku")),
            existing=ledger_balances("sku"),
        )
        with pytest.raises(PGCraftValidationError, match="existing"):
            _validate_events([event], root)

    def test_diff_key_not_a_column_raises(self):
        root, _ = _make_ledger_root()
        event = LedgerEvent(
            name="rec",
            input=lambda p: select(p("x", String).label("x")),
            desired=lambda pginput: select(pginput.c.x),
            diff_keys=["nonexistent"],
        )
        with pytest.raises(PGCraftValidationError, match="nonexistent"):
            _validate_events([event], root)

    def test_valid_simple_event_passes(self):
        root, _ = _make_ledger_root()
        event = LedgerEvent(
            name="adjust",
            input=lambda p: select(
                p("sku", String).label("sku"),
                p("value", Integer).label("value"),
            ),
        )
        _validate_events([event], root)  # must not raise

    def test_valid_diff_event_passes(self):
        root, _ = _make_ledger_root()
        event = LedgerEvent(
            name="rec",
            input=lambda p: select(
                p("sku", String).label("sku"),
                p("value", Integer).label("value"),
            ),
            desired=lambda pginput: select(pginput.c.sku, pginput.c.value),
            existing=ledger_balances("sku"),
            diff_keys=["sku"],
        )
        _validate_events([event], root)  # must not raise


class TestLedgerBalances:
    def test_returns_callable(self):
        result = ledger_balances("warehouse", "sku")
        assert callable(result)


class TestLedgerActionsPluginWiring:
    def test_simple_event_generates_function(self):
        from sqlalchemy_declarative_extensions import View

        root, ctx = _make_ledger_root(schema_items=[Column("sku", String)])
        ctx["api"] = View("inventory", "SELECT 1", schema="api")

        event = LedgerEvent(
            name="adjust",
            input=lambda p: select(
                p("sku", String).label("sku"),
                p("value", Integer).label("value"),
            ),
        )
        plugin = LedgerActionsPlugin(events=[event])
        plugin.run(ctx)

        fns = ctx.metadata.info.get("functions")
        assert fns is not None
        fn_names = [f.name for f in fns.functions]
        assert any("adjust" in n for n in fn_names)

    def test_diff_event_generates_function(self):
        from sqlalchemy_declarative_extensions import View

        root, ctx = _make_ledger_root(
            schema_items=[
                Column("sku", String),
                Column("warehouse", String),
            ]
        )
        ctx["api"] = View("inventory", "SELECT 1", schema="api")

        event = LedgerEvent(
            name="reconcile",
            input=lambda p: select(
                p("warehouse", String).label("warehouse"),
                p("sku", String).label("sku"),
                p("value", Integer).label("value"),
            ),
            desired=lambda pginput: select(
                pginput.c.warehouse,
                pginput.c.sku,
                pginput.c.value,
            ),
            existing=ledger_balances("warehouse", "sku"),
            diff_keys=["warehouse", "sku"],
        )
        plugin = LedgerActionsPlugin(events=[event])
        plugin.run(ctx)

        fns = ctx.metadata.info.get("functions")
        assert fns is not None
        fn_names = [f.name for f in fns.functions]
        assert any("reconcile" in n for n in fn_names)
        # Should use LANGUAGE sql, SETOF return.
        fn = next(f for f in fns.functions if "reconcile" in f.name)
        assert fn.language == "sql"
        assert "SETOF" in fn.returns
