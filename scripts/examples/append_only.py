"""Append-only dimension example for documentation generation."""

from sqlalchemy import Column, MetaData, String

from pgcraft import pgcraft_build_naming_conventions
from pgcraft.factory.dimension import (
    AppendOnlyDimensionResourceFactory,
)

metadata = MetaData(naming_convention=pgcraft_build_naming_conventions())

# --- example start ---
AppendOnlyDimensionResourceFactory(
    tablename="employees",
    schemaname="private",
    metadata=metadata,
    schema_items=[
        Column("name", String, nullable=False),
        Column("department", String),
    ],
)
# --- example end ---

SCHEMA_DESCRIPTION = (
    "An append-only attributes table"
    " (``private.employees_attributes``) stores every"
    " version of each row. A root table"
    " (``private.employees_root``) points to the"
    " current version. Two views join these into a"
    " flat shape: ``private.employees`` and"
    " ``api.employees``."
)

VIEWS = [
    {
        "fullname": "private.employees",
        "columns": [
            ("id", "INTEGER", "PK"),
            ("created_at", "DATETIME", ""),
            ("updated_at", "DATETIME", ""),
            ("name", "VARCHAR", "NOT NULL"),
            ("department", "VARCHAR", ""),
        ],
    },
    {
        "fullname": "api.employees",
        "columns": [
            ("id", "INTEGER", "PK"),
            ("created_at", "DATETIME", ""),
            ("updated_at", "DATETIME", ""),
            ("name", "VARCHAR", "NOT NULL"),
            ("department", "VARCHAR", ""),
        ],
    },
]

EXTRA_EDGES = [
    (
        "private.employees",
        "private.employees_root",
        "style=dashed",
    ),
    (
        "private.employees",
        "private.employees_attributes",
        "style=dashed",
    ),
    (
        "api.employees",
        "private.employees",
        'style=dashed label="SELECT *"',
    ),
]

SEED_FILE = "append_only_seed.sql"

QUERIES = [
    {
        "query": ("SELECT * FROM private.employees_attributes ORDER BY id;"),
        "description": (
            "Each change appends a new row.  Row 3 "
            "records Alice's department change."
        ),
    },
    {
        "query": "SELECT * FROM private.employees_root;",
        "description": (
            "Points to the latest attribute row via the foreign key."
        ),
    },
    {
        "query": "SELECT * FROM api.employees;",
        "description": (
            "Joins root with the pointed-to attribute "
            "row, showing the latest values."
        ),
    },
]
