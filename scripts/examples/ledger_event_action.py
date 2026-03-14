"""EventAction example for documentation."""

from sqlalchemy import Column, MetaData, String

from pgcraft.factory.ledger import LedgerResourceFactory
from pgcraft.ledger.actions import EventAction, StateAction
from pgcraft.plugins.ledger import LedgerBalanceViewPlugin
from pgcraft.utils.naming_convention import build_naming_convention

metadata = MetaData(naming_convention=build_naming_convention())

# --- example start ---
reconcile = StateAction(
    name="reconcile",
    diff_keys=["warehouse", "sku"],
)

adjust = EventAction(
    name="adjust",
    # dim_keys not set: inherits ["warehouse", "sku"] from sibling StateAction
    write_only_keys=["reason"],
)

LedgerResourceFactory(
    tablename="inventory",
    schemaname="ops",
    metadata=metadata,
    schema_items=[
        Column("warehouse", String, nullable=False),
        Column("sku", String, nullable=False),
        Column("reason", String, nullable=True),
    ],
    extra_plugins=[
        LedgerBalanceViewPlugin(dimensions=["warehouse", "sku"]),
    ],
    actions=[reconcile, adjust],
)
# --- example end ---
