"""Append-only dimension with constraints and indices for docs."""

from sqlalchemy import Column, Integer, MetaData, String

from pgcraft import pgcraft_build_naming_conventions
from pgcraft.check import PGCraftCheck
from pgcraft.factory import PGCraftAppendOnly, PGCraftSimple
from pgcraft.fk import PGCraftFK
from pgcraft.index import PGCraftIndex

metadata = MetaData(naming_convention=pgcraft_build_naming_conventions())

# --- example start ---
departments = PGCraftSimple(
    tablename="departments",
    schemaname="public",
    metadata=metadata,
    schema_items=[
        Column("name", String, nullable=False),
        PGCraftIndex("uq_departments_name", "{name}", unique=True),
    ],
)

employees = PGCraftAppendOnly(
    tablename="employees",
    schemaname="public",
    metadata=metadata,
    schema_items=[
        Column("name", String, nullable=False),
        Column("salary", Integer, nullable=False),
        Column("department_id", Integer, nullable=False),
        PGCraftCheck("{salary} > 0", name="positive_salary"),
        PGCraftIndex(
            "idx_employees_department_id",
            "{department_id}",
        ),
        PGCraftFK(
            references={"{department_id}": "departments.id"},
            name="fk_employees_department",
        ),
    ],
)
# --- example end ---

SCHEMA_DESCRIPTION = (
    "An append-only ``employees`` dimension with a check"
    " constraint, index, and foreign key to a simple"
    " ``departments`` dimension.  The FK resolves to the"
    " departments table via the dimension registry."
    " Constraints and indices are placed on the"
    " attributes table."
)

VIEWS: list[dict] = []
EXTRA_EDGES: list[tuple] = []

SEED_FILE = "constraints_append_only_seed.sql"

QUERIES = [
    {
        "query": "SELECT * FROM public.departments;",
    },
    {
        "query": ("SELECT * FROM public.employees_attributes ORDER BY id;"),
        "description": (
            "The attributes table has the check constraint, index, and FK."
        ),
    },
    {
        "query": (
            "INSERT INTO"
            " public.employees_attributes"
            " (name, salary, department_id)"
            " VALUES"
            " ('Charlie', -100, 1);"
        ),
        "description": (
            "Inserting an employee with a negative"
            " salary violates the"
            " ``positive_salary`` check constraint."
        ),
        "expect_error": True,
    },
    {
        "query": (
            "INSERT INTO"
            " public.employees_attributes"
            " (name, salary, department_id)"
            " VALUES"
            " ('Charlie', 50000, 999);"
        ),
        "description": (
            "Inserting with a nonexistent department violates the foreign key."
        ),
        "expect_error": True,
    },
]
