"""Simple dimension with constraints and indices for docs."""

from sqlalchemy import Column, Integer, MetaData, Numeric, String

from pgcraft import pgcraft_build_naming_conventions
from pgcraft.check import PGCraftCheck
from pgcraft.factory import PGCraftSimple
from pgcraft.fk import PGCraftFK
from pgcraft.index import PGCraftIndex

metadata = MetaData(naming_convention=pgcraft_build_naming_conventions())

# --- example start ---
customers = PGCraftSimple(
    tablename="customers",
    schemaname="public",
    metadata=metadata,
    schema_items=[
        Column("name", String, nullable=False),
        Column("email", String, nullable=False),
        PGCraftIndex("uq_customers_email", "{email}", unique=True),
    ],
)

orders = PGCraftSimple(
    tablename="orders",
    schemaname="public",
    metadata=metadata,
    schema_items=[
        Column("customer_id", Integer, nullable=False),
        Column("total", Numeric(10, 2), nullable=False),
        Column("status", String, nullable=False),
        PGCraftCheck("{total} > 0", name="positive_total"),
        PGCraftCheck(
            "{status} IN ('pending', 'paid', 'cancelled')",
            name="valid_status",
        ),
        PGCraftIndex("idx_orders_customer_id", "{customer_id}"),
        PGCraftIndex("idx_orders_status", "{status}"),
        PGCraftFK(
            references={"{customer_id}": "customers.id"},
            name="fk_orders_customer",
            ondelete="CASCADE",
        ),
    ],
)
# --- example end ---

SCHEMA_DESCRIPTION = (
    "Two simple tables: ``public.customers`` with a unique"
    " index on email, and ``public.orders`` with check"
    " constraints, indices, and a foreign key to customers."
)

VIEWS: list[dict] = []
EXTRA_EDGES: list[tuple] = []

SEED_FILE = "constraints_simple_seed.sql"

QUERIES = [
    {
        "query": "SELECT * FROM public.customers;",
    },
    {
        "query": "SELECT * FROM public.orders;",
    },
    {
        "query": (
            "INSERT INTO public.orders"
            " (customer_id, total, status)"
            " VALUES (1, -5, 'pending');"
        ),
        "description": (
            "Inserting an order with a negative total"
            " violates the ``positive_total`` check"
            " constraint."
        ),
        "expect_error": True,
    },
    {
        "query": (
            "INSERT INTO public.orders"
            " (customer_id, total, status)"
            " VALUES (999, 10, 'pending');"
        ),
        "description": (
            "Inserting an order with a nonexistent"
            " customer violates the foreign key."
        ),
        "expect_error": True,
    },
]
