"""Diff event (reconciliation) example for documentation."""

from sqlalchemy import Column, Integer, MetaData, String, select

from pgcraft.factory.ledger import LedgerResourceFactory
from pgcraft.ledger.events import LedgerEvent, ledger_balances
from pgcraft.plugins.ledger import LedgerBalanceViewPlugin
from pgcraft.utils.naming_convention import build_naming_convention

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

LedgerResourceFactory(
    tablename="inventory",
    schemaname="ops",
    metadata=metadata,
    schema_items=[
        Column("warehouse", String, nullable=False),
        Column("sku", String, nullable=False),
        Column("source", String, nullable=True),
    ],
    extra_plugins=[
        LedgerBalanceViewPlugin(dimensions=["warehouse", "sku"]),
    ],
    events=[reconcile],
)
# --- example end ---
