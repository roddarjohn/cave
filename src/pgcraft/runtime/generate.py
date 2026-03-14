"""Generate a safe, sorted list of Alembic ops from a MetaData and connection.

:func:`generate_ops` is the autogenerate step of the runtime pipeline.  It
diffs the desired schema (expressed as a SQLAlchemy ``MetaData``) against
the live database, restricted to a single PostgreSQL schema so it cannot
accidentally touch other tenants' data.

The returned op list is sorted by dependency order (extensions first, then
schemas, then tables, etc.) but is **not yet filtered**.  Callers must pass
the result through :func:`~pgcraft.runtime.filter.filter_safe_ops` before
applying it.

**Prerequisites**

:func:`~pgcraft.alembic.register.pgcraft_alembic_hook` must have been called
once before using this function.  It registers the autogenerate comparators
(for views, triggers, grants, extensions, etc.) that produce the full set of
ops from the MetaData.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from alembic.autogenerate import produce_migrations
from alembic.runtime.migration import MigrationContext

from pgcraft.alembic.dependency import AnyOp, sort_migration_ops

if TYPE_CHECKING:
    from sqlalchemy import Connection, MetaData


def _make_include_name(
    schema: str,
) -> object:
    """Return an Alembic ``include_name`` callback scoped to *schema*.

    Alembic calls this for every object it discovers during introspection.
    Returning ``False`` tells it to ignore the object entirely.

    Args:
        schema: The PostgreSQL schema to restrict comparison to.

    Returns:
        A callable suitable for ``MigrationContext.configure(opts=...)``.

    """

    def include_name(
        name: str | None,
        type_: str,
        parent_names: dict[str, str | None],
    ) -> bool:
        if type_ == "schema":
            return name == schema
        return parent_names.get("schema_name") == schema

    return include_name


def generate_ops(
    conn: Connection,
    metadata: MetaData,
    schema: str,
) -> list[AnyOp]:
    """Diff *metadata* against the live database and return sorted ops.

    Runs Alembic autogenerate restricted to *schema*, collects the resulting
    upgrade ops, and returns them sorted in dependency order (using
    :func:`~pgcraft.alembic.dependency.sort_migration_ops`).

    The returned list may contain destructive ops if the live schema is wider
    than the declared config.  Always pass the result through
    :func:`~pgcraft.runtime.filter.filter_safe_ops` before applying.

    Args:
        conn: An active SQLAlchemy connection to the target database.
        metadata: The desired schema state, produced by
            :func:`~pgcraft.runtime.builder.build_metadata`.
        schema: The PostgreSQL schema to scope the comparison to.

    Returns:
        Ordered list of :class:`~alembic.operations.MigrateOperation`
        instances representing the delta between desired and live state.

    """
    mc = MigrationContext.configure(
        conn,
        opts={
            "target_metadata": metadata,
            "include_schemas": True,
            "include_name": _make_include_name(schema),
        },
    )
    script = produce_migrations(mc, metadata)
    upgrade_ops = script.upgrade_ops
    if upgrade_ops is None:
        return []
    raw_ops: list[AnyOp] = list(upgrade_ops.ops)
    return sort_migration_ops(raw_ops)
