"""Double-entry ledger example for documentation generation."""

from sqlalchemy import Column, MetaData, String

from pgcraft.factory.ledger import LedgerResourceFactory
from pgcraft.plugins.ledger import (
    DoubleEntryPlugin,
    DoubleEntryTriggerPlugin,
)
from pgcraft.utils.naming_convention import build_naming_convention

metadata = MetaData(naming_convention=build_naming_convention())

# --- example start ---
LedgerResourceFactory(
    tablename="journal",
    schemaname="finance",
    metadata=metadata,
    schema_items=[
        Column("account", String, nullable=False),
    ],
    extra_plugins=[
        DoubleEntryPlugin(),
        DoubleEntryTriggerPlugin(),
    ],
)
# --- example end ---

SCHEMA_DESCRIPTION = (
    "A double-entry journal table (``finance.journal``)"
    " adds a ``direction`` column (``'debit'`` or"
    " ``'credit'``) to the standard ledger. An"
    " ``AFTER INSERT`` constraint trigger validates that"
    " debits equal credits for every ``entry_id`` in the"
    " batch."
)

VIEWS = [
    {
        "fullname": "api.journal",
        "columns": [
            ("id", "INTEGER", "PK"),
            ("entry_id", "UUID", "NOT NULL"),
            ("created_at", "DATETIME", ""),
            ("value", "INTEGER", "NOT NULL"),
            ("direction", "VARCHAR(6)", "NOT NULL"),
            ("account", "VARCHAR", "NOT NULL"),
        ],
    },
]

EXTRA_EDGES = [
    (
        "api.journal",
        "finance.journal",
        'style=dashed label="SELECT *"',
    ),
]

SEED_FILE = "double_entry_seed.sql"

QUERIES = [
    {
        "query": "SELECT * FROM finance.journal ORDER BY id;",
        "description": (
            "Each entry_id group must balance: total debits = total credits."
        ),
    },
    {
        "query": (
            "SELECT account, direction,"
            " SUM(value) AS total"
            " FROM finance.journal"
            " GROUP BY account, direction"
            " ORDER BY account, direction;"
        ),
        "description": (
            "Aggregating by account and direction"
            " shows the T-account breakdown."
        ),
    },
]
