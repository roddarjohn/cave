"""DDL safety filter for runtime-applied migrations.

Autogenerate can produce destructive operations (DROP TABLE, DROP COLUMN,
ALTER COLUMN, etc.) when the declared schema is narrower than the live
database schema.  This module provides :func:`filter_safe_ops`, which
raises :class:`~pgcraft.errors.DestructiveOperationError` for any such
operation before anything is executed.

Design principles:

- **Closed-world allowlist.**  Any op type not explicitly listed is
  rejected.  If Alembic or sqlalchemy-declarative-extensions introduces a
  new op type, it will fail loudly here until it is reviewed and
  consciously placed in the allowlist or blocklist.  Silent pass-through
  of unknown ops is never acceptable.

- **Raise, never skip.**  Destructive ops are not silently dropped from
  the list.  They raise an exception so the caller can surface a clear
  error, log it, and mark the registry entry as failed.  Skipping would
  mask the root cause (a config that conflicts with the live schema).

- **``ModifyTableOps`` is decomposed.**  Alembic groups column-level
  changes under a single ``ModifyTableOps`` wrapper.  The filter inspects
  every inner operation and raises if any are destructive, even if others
  in the same wrapper would be safe.
"""

from __future__ import annotations

from alembic.operations import ops as alembic_ops
from sqlalchemy_declarative_extensions.alembic.function import (
    CreateFunctionOp,
    DropFunctionOp,
    UpdateFunctionOp,
)
from sqlalchemy_declarative_extensions.alembic.procedure import (
    CreateProcedureOp,
    DropProcedureOp,
    UpdateProcedureOp,
)
from sqlalchemy_declarative_extensions.alembic.schema import (
    CreateSchemaOp,
    DropSchemaOp,
)
from sqlalchemy_declarative_extensions.alembic.trigger import (
    CreateTriggerOp,
    DropTriggerOp,
    UpdateTriggerOp,
)
from sqlalchemy_declarative_extensions.alembic.view import (
    CreateViewOp,
    DropViewOp,
    UpdateViewOp,
)
from sqlalchemy_declarative_extensions.grant.compare import (
    GrantPrivilegesOp,
    RevokePrivilegesOp,
)
from sqlalchemy_declarative_extensions.role.compare import (
    CreateRoleOp,
    DropRoleOp,
)

from pgcraft.alembic.extension import CreateExtensionOp
from pgcraft.errors import DestructiveOperationError

# Operations that are unconditionally safe to apply at runtime.
# These are all additive or idempotent: they create or replace objects
# but never remove data or break existing consumers of the schema.
_ALLOWED_OP_TYPES: tuple[type, ...] = (
    CreateExtensionOp,
    CreateSchemaOp,
    alembic_ops.CreateTableOp,
    CreateViewOp,
    UpdateViewOp,
    CreateFunctionOp,
    UpdateFunctionOp,
    CreateProcedureOp,
    UpdateProcedureOp,
    CreateTriggerOp,
    UpdateTriggerOp,
    CreateRoleOp,
    GrantPrivilegesOp,
)

# Operations that are unconditionally destructive.
# Listed explicitly so that the intent is clear, even though unknown ops
# are also rejected by the closed-world allowlist.
_BLOCKED_OP_TYPES: tuple[type, ...] = (
    DropSchemaOp,
    alembic_ops.DropTableOp,
    DropViewOp,
    DropFunctionOp,
    DropProcedureOp,
    DropTriggerOp,
    DropRoleOp,
    RevokePrivilegesOp,
    alembic_ops.ExecuteSQLOp,  # opaque — cannot verify safety
)

# Inner ops inside ModifyTableOps that are safe.
_ALLOWED_INNER_OP_TYPES: tuple[type, ...] = (alembic_ops.AddColumnOp,)


def _check_modify_table(op: alembic_ops.ModifyTableOps) -> None:
    """Raise if any inner op inside a ModifyTableOps is destructive.

    Args:
        op: The ``ModifyTableOps`` container to inspect.

    Raises:
        DestructiveOperationError: If any inner op is not in
            ``_ALLOWED_INNER_OP_TYPES``.

    """
    for inner in op.ops:
        if not isinstance(inner, _ALLOWED_INNER_OP_TYPES):
            raise DestructiveOperationError(inner)


def filter_safe_ops(
    ops: list[alembic_ops.MigrateOperation],
) -> list[alembic_ops.MigrateOperation]:
    """Return *ops* unchanged after verifying every op is safe to apply.

    Iterates the op list and raises :class:`~pgcraft.errors.DestructiveOperationError`
    on the first op that is not in the allowlist.  If all ops are safe the
    original list is returned unmodified so callers can chain this call
    directly into the apply step.

    Args:
        ops: Ordered list of Alembic migration operations, typically
            produced by autogenerate and sorted by
            :func:`~pgcraft.alembic.dependency.sort_migration_ops`.

    Returns:
        The same list, verified safe.

    Raises:
        DestructiveOperationError: On the first op that is destructive or
            unrecognised.

    """
    for op in ops:
        if isinstance(op, _ALLOWED_OP_TYPES):
            continue
        if isinstance(op, _BLOCKED_OP_TYPES):
            raise DestructiveOperationError(op)
        if isinstance(op, alembic_ops.ModifyTableOps):
            _check_modify_table(op)
            continue
        # Closed-world: any op type not explicitly handled above is rejected.
        raise DestructiveOperationError(op)

    return ops
