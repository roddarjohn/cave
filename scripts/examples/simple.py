"""Simple dimension example for documentation generation."""

from sqlalchemy import Column, MetaData, String

from pgcraft import pgcraft_build_naming_conventions
from pgcraft.factory.dimension import SimpleDimensionResourceFactory

metadata = MetaData(naming_convention=pgcraft_build_naming_conventions())

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

SCHEMA_DESCRIPTION = (
    "A single backing table (``public.users``) with"
    " a thin ``api.users`` view on top."
)

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
