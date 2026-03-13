"""Ledger example for documentation generation."""

from sqlalchemy import Column, MetaData, String

from pgcraft.factory.ledger import LedgerResourceFactory
from pgcraft.plugins.ledger import LedgerLatestViewPlugin
from pgcraft.utils.naming_convention import build_naming_convention

metadata = MetaData(naming_convention=build_naming_convention())

# --- example start ---
LedgerResourceFactory(
    tablename="order_events",
    schemaname="ops",
    metadata=metadata,
    schema_items=[
        Column("order_id", String, nullable=False),
        Column("status", String, nullable=False),
    ],
    extra_plugins=[
        LedgerLatestViewPlugin(dimensions=["order_id"]),
    ],
)
# --- example end ---

SCHEMA_DESCRIPTION = (
    "An append-only event log (``ops.order_events``) records"
    " immutable status transitions. Each row carries a"
    " ``value`` (typically ``1``), an ``entry_id`` UUID, and"
    " dimension columns. The ``ops.order_events_latest``"
    " view shows the most recent event per ``order_id``."
    " An ``api.order_events`` view exposes SELECT and"
    " INSERT only."
)

VIEWS = [
    {
        "fullname": "api.order_events",
        "columns": [
            ("id", "INTEGER", "PK"),
            ("entry_id", "UUID", "NOT NULL"),
            ("created_at", "DATETIME", ""),
            ("value", "INTEGER", "NOT NULL"),
            ("order_id", "VARCHAR", "NOT NULL"),
            ("status", "VARCHAR", "NOT NULL"),
        ],
    },
]

EXTRA_EDGES = [
    (
        "api.order_events",
        "ops.order_events",
        'style=dashed label="SELECT *"',
    ),
]

SEED_FILE = "ledger_seed.sql"

QUERIES = [
    {
        "query": ("SELECT * FROM ops.order_events ORDER BY id;"),
        "description": (
            "All entries are immutable.  Once inserted,"
            " rows cannot be updated or deleted."
        ),
    },
    {
        "query": ("SELECT * FROM ops.order_events_latest ORDER BY order_id;"),
        "description": (
            "The latest view shows the most recent event per order."
        ),
    },
    {
        "query": (
            "SELECT order_id, COUNT(*) AS transitions"
            " FROM ops.order_events"
            " GROUP BY order_id"
            " ORDER BY order_id;"
        ),
        "description": (
            "Counting events per order shows how many"
            " state transitions each order has gone through."
        ),
    },
]
