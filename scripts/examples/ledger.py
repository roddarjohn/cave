"""Ledger example for documentation generation."""

from sqlalchemy import Column, MetaData, String

from pgcraft.factory.ledger import LedgerResourceFactory
from pgcraft.utils.naming_convention import build_naming_convention

metadata = MetaData(naming_convention=build_naming_convention())

# --- example start ---
LedgerResourceFactory(
    tablename="transactions",
    schemaname="finance",
    metadata=metadata,
    schema_items=[
        Column("account", String, nullable=False),
        Column("category", String),
    ],
)
# --- example end ---

SCHEMA_DESCRIPTION = (
    "A single append-only table (``finance.transactions``)"
    " stores immutable ledger entries. Each row has a"
    " ``value`` column, an ``entry_id`` UUID for correlating"
    " related entries, and consumer-provided dimension"
    " columns. An ``api.transactions`` view exposes SELECT"
    " and INSERT only."
)

VIEWS = [
    {
        "fullname": "api.transactions",
        "columns": [
            ("id", "INTEGER", "PK"),
            ("entry_id", "UUID", "NOT NULL"),
            ("created_at", "DATETIME", ""),
            ("value", "INTEGER", "NOT NULL"),
            ("account", "VARCHAR", "NOT NULL"),
            ("category", "VARCHAR", ""),
        ],
    },
]

EXTRA_EDGES = [
    (
        "api.transactions",
        "finance.transactions",
        'style=dashed label="SELECT *"',
    ),
]

SEED_FILE = "ledger_seed.sql"

QUERIES = [
    {
        "query": "SELECT * FROM finance.transactions ORDER BY id;",
        "description": (
            "All entries are immutable.  Once inserted,"
            " rows cannot be updated or deleted."
        ),
    },
    {
        "query": "SELECT * FROM api.transactions ORDER BY id;",
        "description": "The API view exposes the same columns.",
    },
    {
        "query": (
            "SELECT account, SUM(value) AS balance"
            " FROM finance.transactions"
            " GROUP BY account"
            " ORDER BY account;"
        ),
        "description": (
            "Balances are derived by summing value per dimension group."
        ),
    },
]
