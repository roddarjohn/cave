"""StateAction example for documentation."""

from sqlalchemy import Column, MetaData, String

from pgcraft.factory.ledger import LedgerResourceFactory
from pgcraft.ledger.actions import StateAction
from pgcraft.plugins.ledger import LedgerBalanceViewPlugin
from pgcraft.utils.naming_convention import build_naming_convention

metadata = MetaData(naming_convention=build_naming_convention())

# --- example start ---
reconcile = StateAction(
    name="reconcile",
    diff_keys=["warehouse", "sku"],
    partial=False,
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
    actions=[reconcile],
)
# --- example end ---
