"""Autogenerate comparator for PostgreSQL extensions.

Compares the extension names declared in
``metadata.info["pgcraft_extensions"]`` against the extensions installed
in the live database, and emits :class:`CreateExtensionOp` for any that
are missing.

:class:`CreateExtensionOp` is a typed operation (rather than a raw
``ExecuteSQLOp``) so that :func:`~pgcraft.alembic.dependency.sort_migration_ops`
can recognise it and guarantee it is placed before any table or schema op that
depends on the extension.

No downgrade op is emitted — dropping an extension is rarely safe to do
automatically because other schema objects may depend on it.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from alembic.autogenerate import comparators
from alembic.autogenerate.render import renderers
from alembic.operations import MigrateOperation
from sqlalchemy import text

if TYPE_CHECKING:
    from alembic.autogenerate.api import AutogenContext
    from alembic.operations.ops import UpgradeOps
    from sqlalchemy import MetaData


class CreateExtensionOp(MigrateOperation):
    """Autogenerate op that emits ``CREATE EXTENSION IF NOT EXISTS <name>``.

    Using a dedicated type (rather than a plain ``ExecuteSQLOp``) lets the
    migration dependency sorter identify these ops and guarantee they are
    ordered before any table or schema that depends on the extension.

    Args:
        name: The PostgreSQL extension name (e.g. ``"pg_uuidv7"``).

    """

    def __init__(self, name: str) -> None:
        """Store the extension name."""
        self.name = name


def collect_extensions(metadata: MetaData) -> set[str]:
    """Return PostgreSQL extension names declared on *metadata*.

    Args:
        metadata: The SQLAlchemy MetaData to inspect.

    Returns:
        Set of extension names registered by plugins via
        ``metadata.info["pgcraft_extensions"]``.

    """
    return set(metadata.info.get("pgcraft_extensions", set()))


def register_extension_comparator() -> None:
    """Register the autogenerate comparator and renderer for extensions.

    Call this once during Alembic initialisation (i.e. inside
    :func:`~pgcraft.alembic.register.pgcraft_alembic_hook`).  After
    registration, ``alembic revision --autogenerate`` will:

    1. Query ``pg_extension`` and emit a :class:`CreateExtensionOp` for
       every extension declared by a plugin that is not yet installed.
    2. Render each op as ``op.execute("CREATE EXTENSION IF NOT EXISTS ...")``
       in the migration file.

    Ordering is guaranteed by the pgcraft rewriter: :class:`CreateExtensionOp`
    instances are pulled to the front of every upgrade migration by
    :func:`~pgcraft.alembic.dependency.sort_migration_ops`, so the extension
    is always available before any table that uses it.

    The comparator runs only once per autogenerate pass (guarded by the
    presence of ``None`` in the ``schemas`` set, which Alembic always
    includes for the default schema comparison).

    """

    @comparators.dispatch_for("schema")
    def _compare_extensions(
        autogen_context: AutogenContext,
        upgrade_ops: UpgradeOps,
        schemas: list[str | None],
    ) -> None:
        # Guard: run once, during the default-schema comparison pass.
        if None not in schemas:
            return

        metadata: MetaData | None = autogen_context.opts.get("target_metadata")
        if metadata is None:
            return

        desired = collect_extensions(metadata)
        if not desired:
            return

        if autogen_context.connection is None:
            return

        installed: set[str] = {
            row[0]
            for row in autogen_context.connection.execute(
                text("SELECT extname FROM pg_extension")
            )
        }

        for ext in sorted(desired - installed):
            upgrade_ops.ops.append(CreateExtensionOp(ext))

    @renderers.dispatch_for(CreateExtensionOp, replace=True)
    def _render_create_extension(
        _autogen_context: AutogenContext,
        op: CreateExtensionOp,
    ) -> list[str]:
        return [f'op.execute("CREATE EXTENSION IF NOT EXISTS {op.name}")']
