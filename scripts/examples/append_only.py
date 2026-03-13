"""Append-only dimension example for documentation generation."""

from sqlalchemy import Column, MetaData, String

from cave.factory.dimension import (
    AppendOnlyDimensionResourceFactory,
)
from cave.utils.naming_convention import build_naming_convention

metadata = MetaData(naming_convention=build_naming_convention())

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

SAMPLES = [
    {
        "query": "SELECT * FROM private.employees_attributes;",
        "description": (
            "Each change appends a new row.  Row 3 "
            "records Alice's department change."
        ),
        "headers": [
            "id",
            "created_at",
            "name",
            "department",
        ],
        "rows": [
            [
                "1",
                "2024-01-15 10:00",
                "Alice",
                "Engineering",
            ],
            [
                "2",
                "2024-01-15 10:00",
                "Bob",
                "Marketing",
            ],
            [
                "3",
                "2024-03-01 09:00",
                "Alice",
                "Management",
            ],
        ],
    },
    {
        "query": "SELECT * FROM private.employees_root;",
        "description": (
            "Points to the latest attribute row via the foreign key."
        ),
        "headers": [
            "id",
            "created_at",
            "employees_attributes_id",
        ],
        "rows": [
            ["1", "2024-01-15 10:00", "3"],
            ["2", "2024-01-15 10:00", "2"],
        ],
    },
    {
        "query": "SELECT * FROM api.employees;",
        "description": (
            "Joins root with the pointed-to attribute "
            "row, showing the latest values."
        ),
        "headers": [
            "id",
            "created_at",
            "updated_at",
            "name",
            "department",
        ],
        "rows": [
            [
                "1",
                "2024-01-15 10:00",
                "2024-03-01 09:00",
                "Alice",
                "Management",
            ],
            [
                "2",
                "2024-01-15 10:00",
                "2024-01-15 10:00",
                "Bob",
                "Marketing",
            ],
        ],
    },
]
