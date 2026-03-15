"""Diff event (reconciliation) example for documentation."""

from sqlalchemy import Column, Integer, MetaData, String, select

from pgcraft.extensions.postgrest import PostgRESTView
from pgcraft.factory import PGCraftLedger
from pgcraft.ledger.events import LedgerEvent, ledger_balances
from pgcraft.utils.naming_convention import build_naming_convention
from pgcraft.views import (
    BalanceView,
    LedgerActions,
)

metadata = MetaData(naming_convention=build_naming_convention())

# --- example start ---
reconcile = LedgerEvent(
    name="reconcile",
    input=lambda p: select(
        p("warehouse", String).label("warehouse"),
        p("sku", String).label("sku"),
        p("value", Integer).label("value"),
        p("source", String).label("source"),
    ),
    desired=lambda pginput: select(
        pginput.c.warehouse,
        pginput.c.sku,
        pginput.c.value,
        pginput.c.source,
    ),
    existing=ledger_balances("warehouse", "sku"),
    diff_keys=["warehouse", "sku"],
)

inventory = PGCraftLedger(
    tablename="inventory",
    schemaname="ops",
    metadata=metadata,
    schema_items=[
        Column("warehouse", String, nullable=False),
        Column("sku", String, nullable=False),
        Column("source", String, nullable=True),
    ],
)

PostgRESTView(source=inventory, grants=["select", "insert"])
BalanceView(
    source=inventory,
    dimensions=["warehouse", "sku"],
)
LedgerActions(source=inventory, events=[reconcile])
# --- example end ---
