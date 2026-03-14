"""Unit tests for ledger action validation and plugin wiring."""

from __future__ import annotations

import pytest
from sqlalchemy import Column, String, Table

from pgcraft.errors import PGCraftValidationError
from pgcraft.ledger.actions import EventAction, StateAction
from pgcraft.plugins.entry_id import UUIDEntryIDPlugin
from pgcraft.plugins.ledger import LedgerTablePlugin
from pgcraft.plugins.ledger_actions import (
    LedgerActionsPlugin,
    _validate_actions,
)
from tests.unit.plugins.conftest import make_ctx


def _make_ledger_root(
    schema_items: list | None = None,
) -> Table:
    """Build a minimal ledger root table for testing validation."""
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
    return ctx["__root__"]


class TestStateActionValidation:
    def test_empty_diff_keys_raises(self):
        root = _make_ledger_root()
        action = StateAction(name="reconcile", diff_keys=[])
        with pytest.raises(PGCraftValidationError, match="diff_keys"):
            _validate_actions([action], root)

    def test_diff_key_not_a_column_raises(self):
        root = _make_ledger_root()
        action = StateAction(name="x", diff_keys=["nonexistent"])
        with pytest.raises(PGCraftValidationError, match="nonexistent"):
            _validate_actions([action], root)

    def test_write_only_overlaps_diff_key_raises(self):
        root = _make_ledger_root()
        action = StateAction(
            name="x", diff_keys=["sku"], write_only_keys=["sku"]
        )
        with pytest.raises(PGCraftValidationError, match="overlaps"):
            _validate_actions([action], root)

    def test_write_only_key_value_raises(self):
        root = _make_ledger_root()
        action = StateAction(
            name="x", diff_keys=["sku"], write_only_keys=["value"]
        )
        with pytest.raises(PGCraftValidationError, match="'value'"):
            _validate_actions([action], root)

    def test_write_only_key_not_a_column_raises(self):
        root = _make_ledger_root()
        action = StateAction(
            name="x",
            diff_keys=["sku"],
            write_only_keys=["not_a_col"],
        )
        with pytest.raises(PGCraftValidationError, match="not_a_col"):
            _validate_actions([action], root)

    def test_valid_state_action_passes(self):
        root = _make_ledger_root()
        action = StateAction(
            name="rec",
            diff_keys=["sku", "warehouse"],
            write_only_keys=["reason"],
        )
        _validate_actions([action], root)  # must not raise


class TestDuplicateActionNames:
    def test_duplicate_names_raise(self):
        root = _make_ledger_root()
        a1 = StateAction(name="sync", diff_keys=["sku"])
        a2 = EventAction(name="sync")
        with pytest.raises(PGCraftValidationError, match="Duplicate"):
            _validate_actions([a1, a2], root)

    def test_unique_names_pass(self):
        root = _make_ledger_root()
        a1 = StateAction(name="sync", diff_keys=["sku"])
        a2 = EventAction(name="record")
        _validate_actions([a1, a2], root)  # must not raise


class TestEventActionValidation:
    def test_write_only_not_a_column_raises(self):
        root = _make_ledger_root()
        action = EventAction(name="record", write_only_keys=["nope"])
        with pytest.raises(PGCraftValidationError, match="nope"):
            _validate_actions([action], root)

    def test_valid_event_action_passes(self):
        root = _make_ledger_root()
        action = EventAction(name="record", write_only_keys=["reason"])
        _validate_actions([action], root)  # must not raise


class TestStateActionPluginWiring:
    def test_staging_table_populated_after_run(self):
        """Private attrs set after plugin.run()."""
        from sqlalchemy_declarative_extensions import View

        ctx = make_ctx(
            tablename="inventory",
            schemaname="ops",
            schema_items=[
                Column("sku", String),
                Column("warehouse", String),
            ],
        )
        UUIDEntryIDPlugin().run(ctx)
        LedgerTablePlugin().run(ctx)
        # Provide a minimal API view in ctx so the plugin can access it.
        ctx["api"] = View("inventory", "SELECT 1", schema="api")

        action = StateAction(name="rec", diff_keys=["sku", "warehouse"])
        plugin = LedgerActionsPlugin(actions=[action])
        plugin.run(ctx)

        assert action._begin_fn is not None  # noqa: SLF001
        assert action._apply_fn is not None  # noqa: SLF001
        assert action._staging_table is not None  # noqa: SLF001
        assert "_inventory_rec" in action._staging_table.name  # noqa: SLF001

    def test_event_action_record_fn_populated(self):
        from sqlalchemy_declarative_extensions import View

        ctx = make_ctx(
            tablename="inventory",
            schemaname="ops",
            schema_items=[Column("sku", String)],
        )
        UUIDEntryIDPlugin().run(ctx)
        LedgerTablePlugin().run(ctx)
        ctx["api"] = View("inventory", "SELECT 1", schema="api")

        action = EventAction(name="record")
        plugin = LedgerActionsPlugin(actions=[action])
        plugin.run(ctx)

        assert action._record_fn is not None  # noqa: SLF001
        assert "record" in action._record_fn  # noqa: SLF001

    def test_event_action_generates_one_function(self):
        """EventAction must register exactly one function on metadata."""
        from sqlalchemy_declarative_extensions import View

        ctx = make_ctx(
            tablename="stock",
            schemaname="ops",
            schema_items=[Column("item", String)],
        )
        UUIDEntryIDPlugin().run(ctx)
        LedgerTablePlugin().run(ctx)
        ctx["api"] = View("stock", "SELECT 1", schema="api")

        action = EventAction(name="adjust")
        plugin = LedgerActionsPlugin(actions=[action])
        plugin.run(ctx)

        fns = ctx.metadata.info.get("functions")
        assert fns is not None
        fn_names = [f.name for f in fns.functions]
        # EventAction generates one function (plus ledger_apply_state).
        assert any("adjust" in n for n in fn_names)
        # No begin/apply — those are StateAction only.
        assert not any("adjust_begin" in n for n in fn_names)
        assert not any("adjust_apply" in n for n in fn_names)
