"""EAV dimension example for documentation generation."""

from sqlalchemy import (
    Boolean,
    Column,
    Float,
    Integer,
    MetaData,
    String,
)

from cave.factory.dimension import EAVDimensionResourceFactory
from cave.utils.naming_convention import build_naming_convention

metadata = MetaData(naming_convention=build_naming_convention())

# --- example start ---
EAVDimensionResourceFactory(
    tablename="products",
    schemaname="private",
    metadata=metadata,
    schema_items=[
        Column("color", String),
        Column("weight", Float),
        Column("is_active", Boolean),
        Column("price", Integer),
    ],
)
# --- example end ---

VIEWS = [
    {
        "fullname": "private.products",
        "columns": [
            ("id", "INTEGER", "PK"),
            ("created_at", "DATETIME", ""),
            ("color", "VARCHAR", ""),
            ("weight", "FLOAT", ""),
            ("is_active", "BOOLEAN", ""),
            ("price", "INTEGER", ""),
        ],
    },
    {
        "fullname": "api.products",
        "columns": [
            ("id", "INTEGER", "PK"),
            ("created_at", "DATETIME", ""),
            ("color", "VARCHAR", ""),
            ("weight", "FLOAT", ""),
            ("is_active", "BOOLEAN", ""),
            ("price", "INTEGER", ""),
        ],
    },
]

EXTRA_EDGES = [
    (
        "private.products",
        "private.products_entity",
        "style=dashed",
    ),
    (
        "private.products",
        "private.products_attribute",
        "style=dashed",
    ),
    (
        "api.products",
        "private.products",
        'style=dashed label="SELECT *"',
    ),
]

SAMPLES = [
    {
        "query": "SELECT * FROM private.products_entity;",
        "description": "One row per logical entity.",
        "headers": ["id", "created_at"],
        "rows": [
            ["1", "2024-01-15 10:00"],
            ["2", "2024-01-15 10:00"],
        ],
    },
    {
        "query": ("SELECT * FROM private.products_attribute ORDER BY id;"),
        "description": (
            "Each attribute is a separate row.  "
            "The check constraint ensures exactly "
            "one value column is non-null per row."
        ),
        "headers": [
            "id",
            "entity_id",
            "attribute_name",
            "string_value",
            "float_value",
            "boolean_value",
            "integer_value",
            "created_at",
        ],
        "rows": [
            [
                "1",
                "1",
                "color",
                "red",
                "NULL",
                "NULL",
                "NULL",
                "2024-01-15",
            ],
            [
                "2",
                "1",
                "weight",
                "NULL",
                "2.5",
                "NULL",
                "NULL",
                "2024-01-15",
            ],
            [
                "3",
                "1",
                "is_active",
                "NULL",
                "NULL",
                "true",
                "NULL",
                "2024-01-15",
            ],
            [
                "4",
                "1",
                "price",
                "NULL",
                "NULL",
                "NULL",
                "999",
                "2024-01-15",
            ],
            [
                "5",
                "2",
                "color",
                "blue",
                "NULL",
                "NULL",
                "NULL",
                "2024-01-15",
            ],
            [
                "6",
                "2",
                "weight",
                "NULL",
                "1.0",
                "NULL",
                "NULL",
                "2024-01-15",
            ],
            [
                "7",
                "2",
                "is_active",
                "NULL",
                "NULL",
                "true",
                "NULL",
                "2024-01-15",
            ],
            [
                "8",
                "2",
                "price",
                "NULL",
                "NULL",
                "NULL",
                "499",
                "2024-01-15",
            ],
        ],
    },
    {
        "query": "SELECT * FROM api.products;",
        "description": (
            "Pivots attribute rows back into "
            "columns.  This is what the API "
            "exposes."
        ),
        "headers": [
            "id",
            "created_at",
            "color",
            "weight",
            "is_active",
            "price",
        ],
        "rows": [
            [
                "1",
                "2024-01-15 10:00",
                "red",
                "2.5",
                "true",
                "999",
            ],
            [
                "2",
                "2024-01-15 10:00",
                "blue",
                "1.0",
                "true",
                "499",
            ],
        ],
    },
]
