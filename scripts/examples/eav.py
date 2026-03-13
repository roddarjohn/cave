"""EAV dimension example for documentation generation."""

from sqlalchemy import (
    Boolean,
    Column,
    Float,
    Integer,
    MetaData,
    String,
)

from pgcraft.factory.dimension import EAVDimensionResourceFactory
from pgcraft.utils.naming_convention import build_naming_convention

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

SCHEMA_DESCRIPTION = (
    "An entity table (``private.products_entity``)"
    " holds one row per logical entity. An attribute"
    " table (``private.products_attribute``) stores"
    " each field as a separate row with typed value"
    " columns. Two pivot views reconstruct the"
    " columnar layout: ``private.products`` and"
    " ``api.products``."
)

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

SEED_FILE = "eav_seed.sql"

QUERIES = [
    {
        "query": ("SELECT * FROM private.products_entity;"),
        "description": "One row per logical entity.",
    },
    {
        "query": ("SELECT * FROM private.products_attribute ORDER BY id;"),
        "description": (
            "Each attribute is a separate row.  "
            "The check constraint ensures exactly "
            "one value column is non-null per row."
        ),
    },
    {
        "query": "SELECT * FROM api.products;",
        "description": (
            "Pivots attribute rows back into "
            "columns.  This is what the API "
            "exposes."
        ),
    },
]
