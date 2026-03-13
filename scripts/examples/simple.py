"""Simple dimension example for documentation generation."""

from sqlalchemy import Column, MetaData, String

from cave.factory.dimension import SimpleDimensionResourceFactory
from cave.utils.naming_convention import build_naming_convention

metadata = MetaData(naming_convention=build_naming_convention())

# --- example start ---
SimpleDimensionResourceFactory(
    tablename="users",
    schemaname="public",
    metadata=metadata,
    schema_items=[
        Column("name", String, nullable=False),
        Column("email", String),
    ],
)
# --- example end ---

VIEWS = [
    {
        "fullname": "api.users",
        "columns": [
            ("id", "INTEGER", "PK"),
            ("name", "VARCHAR", "NOT NULL"),
            ("email", "VARCHAR", ""),
        ],
    },
]

EXTRA_EDGES = [
    (
        "api.users",
        "public.users",
        'style=dashed label="SELECT *"',
    ),
]

SEED_FILE = "simple_seed.sql"

QUERIES = [
    {
        "query": "SELECT * FROM public.users;",
    },
    {
        "query": "SELECT * FROM api.users;",
        "description": ("The API view exposes the same columns."),
    },
]
