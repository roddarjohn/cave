"""EAV dimension with check constraints for docs.

EAV dimensions enforce check constraints via INSTEAD OF trigger
functions rather than table-level CHECK constraints, because the
virtual columns only exist in the pivot view.
"""

from sqlalchemy import Column, Float, Integer, MetaData, String

from pgcraft import pgcraft_build_naming_conventions
from pgcraft.check import PGCraftCheck
from pgcraft.factory import PGCraftEAV
from pgcraft.views import APIView

metadata = MetaData(naming_convention=pgcraft_build_naming_conventions())

# --- example start ---
products = PGCraftEAV(
    tablename="products",
    schemaname="private",
    metadata=metadata,
    schema_items=[
        Column("color", String),
        Column("weight", Float),
        Column("price", Integer),
        PGCraftCheck("{price} > 0", name="positive_price"),
        PGCraftCheck("{weight} > 0", name="positive_weight"),
    ],
)

APIView(source=products)
# --- example end ---

SCHEMA_DESCRIPTION = (
    "An EAV dimension with check constraints."
    " Since EAV columns are virtual (only visible in"
    " the pivot view), constraints are enforced via"
    " INSTEAD OF trigger functions that fire before"
    " the main EAV triggers."
)

VIEWS = [
    {
        "fullname": "private.products",
        "columns": [
            ("id", "INTEGER", "PK"),
            ("created_at", "DATETIME", ""),
            ("color", "VARCHAR", ""),
            ("weight", "FLOAT", ""),
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

SEED_FILE = "constraints_eav_seed.sql"

QUERIES = [
    {
        "query": "SELECT * FROM api.products;",
        "description": (
            "The API view shows the pivot — same as a normal table."
        ),
    },
]
