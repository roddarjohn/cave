"""Automatic schema discovery for alembic autogenerate.

Scans a :class:`~sqlalchemy.MetaData` instance for schemas referenced by
tables and views, then registers them with
``sqlalchemy-declarative-extensions`` so its built-in schema comparator
emits the appropriate ``CREATE SCHEMA`` / ``DROP SCHEMA`` ops.
"""

from sqlalchemy import MetaData
from sqlalchemy_declarative_extensions import Schemas, Views

SYSTEM_SCHEMAS = frozenset(
    {"public", "pg_catalog", "information_schema", "pg_toast"}
)


def collect_schemas(metadata: MetaData) -> set[str]:
    """Return non-system schema names referenced by tables and views."""
    schemas: set[str] = set()

    for table in metadata.tables.values():
        if table.schema is not None:
            schemas.add(table.schema)

    views: Views | None = metadata.info.get("views")
    if views is not None:
        for view in views.views:
            if view.schema is not None:
                schemas.add(view.schema)

    return schemas - SYSTEM_SCHEMAS


def register_schemas(metadata: MetaData) -> None:
    """Populate ``metadata.info["schemas"]`` from tables and views.

    Merges with any schemas already registered on the metadata.
    Safe to call multiple times — existing entries are preserved.
    """
    discovered = collect_schemas(metadata)
    if not discovered:
        return

    existing: Schemas | None = metadata.info.get("schemas")
    already_registered = (
        {s.name for s in existing.schemas} if existing is not None else set()
    )

    new = discovered - already_registered
    if not new:
        return

    base = (
        existing if existing is not None else Schemas(ignore_unspecified=True)
    )
    metadata.info["schemas"] = base.are(*new)
