"""Double-entry ledger example for documentation generation."""

from sqlalchemy import Column, ForeignKey, MetaData, String

from pgcraft import pgcraft_build_naming_conventions
from pgcraft.extensions.postgrest import PostgRESTView
from pgcraft.factory import PGCraftLedger, PGCraftSimple
from pgcraft.plugins.ledger import (
    DoubleEntryPlugin,
    DoubleEntryTriggerPlugin,
)

metadata = MetaData(naming_convention=pgcraft_build_naming_conventions())

# --- example start ---
accounts = PGCraftSimple(
    tablename="accounts",
    schemaname="finance",
    metadata=metadata,
    schema_items=[
        Column("name", String, nullable=False),
        Column("category", String, nullable=False),
    ],
)

PostgRESTView(source=accounts)

journal = PGCraftLedger(
    tablename="journal",
    schemaname="finance",
    metadata=metadata,
    schema_items=[
        Column(
            "account_id",
            ForeignKey("finance.accounts.id"),
            nullable=False,
        ),
    ],
    extra_plugins=[
        DoubleEntryPlugin(),
        DoubleEntryTriggerPlugin(),
    ],
)

PostgRESTView(
    source=journal,
    grants=["select", "insert"],
)
# --- example end ---

SCHEMA_DESCRIPTION = (
    "A double-entry journal table (``finance.journal``)"
    " references an ``accounts`` dimension via FK."
    " The ``direction`` column (``'debit'`` or"
    " ``'credit'``) is added by ``DoubleEntryPlugin``."
    " An ``AFTER INSERT`` constraint trigger validates"
    " that debits equal credits for every ``entry_id``"
    " in the batch."
)

VIEWS = [
    {
        "fullname": "api.accounts",
        "columns": [
            ("id", "INTEGER", "PK"),
            ("name", "VARCHAR", "NOT NULL"),
            ("category", "VARCHAR", "NOT NULL"),
        ],
    },
    {
        "fullname": "api.journal",
        "columns": [
            ("id", "INTEGER", "PK"),
            ("entry_id", "UUID", "NOT NULL"),
            ("created_at", "DATETIME", ""),
            ("value", "INTEGER", "NOT NULL"),
            ("direction", "VARCHAR(6)", "NOT NULL"),
            ("account_id", "INTEGER", "FK NOT NULL"),
        ],
    },
]

EXTRA_EDGES = [
    (
        "api.accounts",
        "finance.accounts",
        'style=dashed label="SELECT *"',
    ),
    (
        "api.journal",
        "finance.journal",
        'style=dashed label="SELECT *"',
    ),
]

SEED_FILE = "double_entry_seed.sql"

QUERIES = [
    {
        "query": "SELECT * FROM finance.accounts ORDER BY id;",
        "description": (
            "The accounts dimension holds the name and"
            " category for each account."
        ),
    },
    {
        "query": "SELECT * FROM finance.journal ORDER BY id;",
        "description": (
            "Each entry_id group must balance: total debits = total credits."
        ),
    },
    {
        "query": (
            "SELECT a.name, a.category, j.direction,"
            " SUM(j.value) AS total"
            " FROM finance.journal j"
            " JOIN finance.accounts a"
            " ON a.id = j.account_id"
            " GROUP BY a.name, a.category, j.direction"
            " ORDER BY a.name, j.direction;"
        ),
        "description": (
            "Joining the journal to the accounts dimension"
            " shows the T-account breakdown with category."
        ),
    },
]
