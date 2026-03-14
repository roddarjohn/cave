"""Apply a filtered op list to a live database and capture the executed SQL.

:func:`apply_ops` is the execution step of the runtime pipeline.  It takes
an op list that has **already been through**
:func:`~pgcraft.runtime.filter.filter_safe_ops` and applies each op to the
database, capturing the raw SQL for the audit log.

Two execution paths are used depending on op type:

- **Standard Alembic ops** (:class:`~alembic.operations.ops.CreateTableOp`,
  :class:`~alembic.operations.ops.ModifyTableOps`) are executed via
  ``Operations.invoke``, Alembic's standard programmatic API.

- **sqlalchemy-declarative-extensions ops** and
  :class:`~pgcraft.alembic.extension.CreateExtensionOp` are not registered
  with Alembic's ``Operations`` dispatcher.  They expose a ``to_sql()``
  method instead, which we call to get executable SQL and run directly.

SQL capture uses SQLAlchemy's ``before_cursor_execute`` connection event,
which fires for every statement regardless of which execution path produced
it — including internal statements from ``Operations.invoke`` implementations.
The listener is installed for the duration of the call and removed in a
``finally`` block so it cannot leak across calls.

Transaction management is the caller's responsibility.  Wrap the call in a
``conn.begin()`` block if you want atomicity.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from alembic.operations import Operations
from alembic.operations import ops as alembic_ops
from alembic.runtime.migration import MigrationContext
from sqlalchemy import Executable, event, text
from sqlalchemy_declarative_extensions.alembic.function import (
    CreateFunctionOp,
    UpdateFunctionOp,
)
from sqlalchemy_declarative_extensions.alembic.procedure import (
    CreateProcedureOp,
    UpdateProcedureOp,
)
from sqlalchemy_declarative_extensions.alembic.schema import CreateSchemaOp
from sqlalchemy_declarative_extensions.alembic.trigger import (
    CreateTriggerOp,
    UpdateTriggerOp,
)
from sqlalchemy_declarative_extensions.alembic.view import (
    CreateViewOp,
    UpdateViewOp,
)
from sqlalchemy_declarative_extensions.grant.compare import GrantPrivilegesOp
from sqlalchemy_declarative_extensions.role.compare import CreateRoleOp

from pgcraft.alembic.extension import CreateExtensionOp

if TYPE_CHECKING:
    from sqlalchemy import Connection

    from pgcraft.alembic.dependency import AnyOp

# Alembic built-in ops that are executable via Operations.invoke.
_NATIVE_OP_TYPES: tuple[type, ...] = (
    alembic_ops.CreateTableOp,
    alembic_ops.ModifyTableOps,
)


def _to_executables(  # noqa: PLR0911
    op: alembic_ops.MigrateOperation,
    conn: Connection,
) -> list[Executable]:
    """Translate a declarative-extensions or pgcraft op to executable SQL.

    Returns a list of SQLAlchemy
    :class:`~sqlalchemy.engine.interfaces.Executable` objects ready to pass
    to ``conn.execute()``.

    Args:
        op: A migration op that is **not** in ``_NATIVE_OP_TYPES``.
        conn: The active connection (required by trigger ops for dialect
            info and by schema ops for potential role switching).

    Returns:
        Non-empty list of statements to execute in order.

    """
    if isinstance(op, CreateExtensionOp):
        return [text(f"CREATE EXTENSION IF NOT EXISTS {op.name}")]

    if isinstance(op, (CreateViewOp, UpdateViewOp)):
        return [text(s) for s in op.to_sql(conn.dialect)]

    if isinstance(op, (CreateTriggerOp, UpdateTriggerOp)):
        return [text(s) for s in op.to_sql(conn)]

    if isinstance(
        op,
        (
            CreateFunctionOp,
            UpdateFunctionOp,
            CreateProcedureOp,
            UpdateProcedureOp,
        ),
    ):
        return [text(s) for s in op.to_sql()]

    if isinstance(op, CreateSchemaOp):
        stmts = op.to_sql()
        return [text(s) if isinstance(s, str) else s for s in stmts]

    if isinstance(op, CreateRoleOp):
        return [text(s) for s in op.to_sql()]

    if isinstance(op, GrantPrivilegesOp):
        # to_sql() returns a TextClause — executable directly.
        return [op.to_sql()]

    # This branch should never be reached if the op list passed through
    # filter_safe_ops first, since that raises on unrecognised types.
    msg = (
        f"apply_ops does not know how to execute {type(op).__name__}. "
        f"Ensure the op list has been through filter_safe_ops first."
    )
    raise TypeError(msg)


def apply_ops(
    conn: Connection,
    ops: list[AnyOp],
) -> str:
    r"""Execute *ops* against *conn* and return the captured SQL.

    Applies each op in order.  Standard Alembic ops are invoked via
    ``Operations``; declarative-extensions ops are applied via their
    ``to_sql()`` method.  All SQL that reaches the database cursor is
    captured via a ``before_cursor_execute`` event listener and returned
    as a single newline-separated string for audit storage.

    Args:
        conn: An active SQLAlchemy connection.  Transaction management is
            the caller's responsibility.
        ops: Ordered, filtered list of ops — must have already passed
            through :func:`~pgcraft.runtime.filter.filter_safe_ops`.

    Returns:
        All SQL statements executed, joined by ``"\n\n"``.  Empty string
        if *ops* is empty.

    """
    captured: list[str] = []

    def _capture(  # noqa: PLR0913
        conn: Connection,  # noqa: ARG001
        cursor: object,  # noqa: ARG001
        statement: str,
        parameters: object,  # noqa: ARG001
        context: object,  # noqa: ARG001
        executemany: bool,  # noqa: ARG001, FBT001
    ) -> None:
        captured.append(statement)

    event.listen(conn, "before_cursor_execute", _capture)
    try:
        mc = MigrationContext.configure(conn)
        operations = Operations(mc)

        for op in ops:
            if isinstance(op, _NATIVE_OP_TYPES):
                operations.invoke(op)  # ty: ignore[no-matching-overload]
            else:
                for stmt in _to_executables(
                    op,  # ty: ignore[invalid-argument-type]
                    conn,
                ):
                    conn.execute(stmt)
    finally:
        event.remove(conn, "before_cursor_execute", _capture)

    return "\n\n".join(captured)
