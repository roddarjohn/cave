"""Unit tests for the DDL safety filter.

Each allowed op type has a test confirming it passes through.
Each blocked op type has a test confirming it raises DestructiveOperationError.
ModifyTableOps is decomposed and each inner op type is tested separately.
Unknown op types are also tested to confirm the closed-world assumption holds.

No database connection is required — filter_safe_ops is a pure function.
"""

import pytest
from alembic.operations import ops as alembic_ops
from sqlalchemy import Column, Integer, Text
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
from sqlalchemy_declarative_extensions.dialects.postgresql import (
    Function,
    Trigger,
)
from sqlalchemy_declarative_extensions.dialects.postgresql.grant import (
    Grant,
    GrantStatement,
    GrantTypes,
)
from sqlalchemy_declarative_extensions.dialects.postgresql.trigger import (
    TriggerEvents,
    TriggerTimes,
)
from sqlalchemy_declarative_extensions.grant.compare import (
    GrantPrivilegesOp,
    RevokePrivilegesOp,
)
from sqlalchemy_declarative_extensions.procedure.base import Procedure
from sqlalchemy_declarative_extensions.role.compare import (
    CreateRoleOp,
    DropRoleOp,
)
from sqlalchemy_declarative_extensions.role.generic import Role
from sqlalchemy_declarative_extensions.schema.base import Schema
from sqlalchemy_declarative_extensions.view.base import View

from pgcraft.alembic.extension import CreateExtensionOp
from pgcraft.errors import DestructiveOperationError
from pgcraft.runtime.filter import filter_safe_ops

# ---------------------------------------------------------------------------
# Helpers — minimal valid objects for each op type
# ---------------------------------------------------------------------------


def _create_schema_op() -> CreateSchemaOp:
    return CreateSchemaOp(Schema("s"))


def _drop_schema_op() -> DropSchemaOp:
    return DropSchemaOp(Schema("s"))


def _view(name: str = "v") -> View:
    return View(name, "SELECT 1", schema="s")


def _function(name: str = "fn") -> Function:
    return Function(name, "SELECT 1", schema="s")


def _procedure(name: str = "proc") -> Procedure:
    return Procedure(name, "BEGIN END", language="plpgsql", schema="s")


def _trigger(name: str = "trg") -> Trigger:
    return Trigger(
        name,
        on="s.t",
        execute="s.fn",
        events=[TriggerEvents.insert],
        time=TriggerTimes.after,
    )


def _role(name: str = "r") -> Role:
    return Role(name)


def _grant_statement() -> GrantStatement:
    g = Grant(grants=("SELECT",), target_role="r")
    return GrantStatement(
        grant=g, grant_type=GrantTypes.table, targets=("s.t",)
    )


def _grant_op() -> GrantPrivilegesOp:
    return GrantPrivilegesOp(_grant_statement())


def _revoke_op() -> RevokePrivilegesOp:
    return RevokePrivilegesOp(_grant_statement())


def _create_table_op(name: str = "t") -> alembic_ops.CreateTableOp:
    return alembic_ops.CreateTableOp(
        name, [Column("id", Integer, primary_key=True)], schema="s"
    )


def _modify_table_ops(
    inner: list[alembic_ops.MigrateOperation],
) -> alembic_ops.ModifyTableOps:
    return alembic_ops.ModifyTableOps("t", inner, schema="s")


def _add_column_op() -> alembic_ops.AddColumnOp:
    return alembic_ops.AddColumnOp("t", Column("x", Text), schema="s")


def _drop_column_op() -> alembic_ops.DropColumnOp:
    return alembic_ops.DropColumnOp("t", "x", schema="s")


def _alter_column_op() -> alembic_ops.AlterColumnOp:
    return alembic_ops.AlterColumnOp("t", "x", schema="s")


# ---------------------------------------------------------------------------
# Allowed op types
# ---------------------------------------------------------------------------


class TestAllowedOps:
    def test_empty_list(self):
        assert filter_safe_ops([]) == []

    def test_create_extension_op(self):
        op = CreateExtensionOp("pg_uuidv7")
        assert filter_safe_ops([op]) == [op]

    def test_create_schema_op(self):
        op = _create_schema_op()
        assert filter_safe_ops([op]) == [op]

    def test_create_table_op(self):
        op = _create_table_op()
        assert filter_safe_ops([op]) == [op]

    def test_create_view_op(self):
        op = CreateViewOp(_view())
        assert filter_safe_ops([op]) == [op]

    def test_update_view_op(self):
        v = _view()
        op = UpdateViewOp(v, v)
        assert filter_safe_ops([op]) == [op]

    def test_create_function_op(self):
        op = CreateFunctionOp(_function())
        assert filter_safe_ops([op]) == [op]

    def test_update_function_op(self):
        fn = _function()
        op = UpdateFunctionOp(fn, fn)
        assert filter_safe_ops([op]) == [op]

    def test_create_procedure_op(self):
        op = CreateProcedureOp(_procedure())
        assert filter_safe_ops([op]) == [op]

    def test_update_procedure_op(self):
        p = _procedure()
        op = UpdateProcedureOp(p, p)
        assert filter_safe_ops([op]) == [op]

    def test_create_trigger_op(self):
        op = CreateTriggerOp(_trigger())
        assert filter_safe_ops([op]) == [op]

    def test_update_trigger_op(self):
        t = _trigger()
        op = UpdateTriggerOp(t, t)
        assert filter_safe_ops([op]) == [op]

    def test_create_role_op(self):
        op = CreateRoleOp(_role())
        assert filter_safe_ops([op]) == [op]

    def test_grant_privileges_op(self):
        op = _grant_op()
        assert filter_safe_ops([op]) == [op]

    def test_modify_table_with_only_add_column(self):
        op = _modify_table_ops([_add_column_op()])
        assert filter_safe_ops([op]) == [op]

    def test_modify_table_with_multiple_add_columns(self):
        op = _modify_table_ops([_add_column_op(), _add_column_op()])
        assert filter_safe_ops([op]) == [op]

    def test_modify_table_empty_inner_ops(self):
        # An empty ModifyTableOps has no destructive inner ops.
        op = _modify_table_ops([])
        assert filter_safe_ops([op]) == [op]

    def test_returns_same_list_object(self):
        # filter_safe_ops must not copy the list — callers chain it directly.
        ops = [_create_table_op(), CreateExtensionOp("pg_uuidv7")]
        result = filter_safe_ops(ops)
        assert result is ops

    def test_preserves_order_of_multiple_ops(self):
        a = CreateExtensionOp("pg_uuidv7")
        b = _create_schema_op()
        c = _create_table_op()
        result = filter_safe_ops([a, b, c])
        assert result == [a, b, c]


# ---------------------------------------------------------------------------
# Blocked op types
# ---------------------------------------------------------------------------


class TestBlockedOps:
    def test_drop_table_op(self):
        op = alembic_ops.DropTableOp("t", schema="s")
        with pytest.raises(DestructiveOperationError) as exc_info:
            filter_safe_ops([op])
        assert exc_info.value.op is op

    def test_drop_schema_op(self):
        op = _drop_schema_op()
        with pytest.raises(DestructiveOperationError) as exc_info:
            filter_safe_ops([op])
        assert exc_info.value.op is op

    def test_drop_view_op(self):
        op = DropViewOp(_view())
        with pytest.raises(DestructiveOperationError) as exc_info:
            filter_safe_ops([op])
        assert exc_info.value.op is op

    def test_drop_function_op(self):
        op = DropFunctionOp(_function())
        with pytest.raises(DestructiveOperationError) as exc_info:
            filter_safe_ops([op])
        assert exc_info.value.op is op

    def test_drop_procedure_op(self):
        op = DropProcedureOp(_procedure())
        with pytest.raises(DestructiveOperationError) as exc_info:
            filter_safe_ops([op])
        assert exc_info.value.op is op

    def test_drop_trigger_op(self):
        op = DropTriggerOp(_trigger())
        with pytest.raises(DestructiveOperationError) as exc_info:
            filter_safe_ops([op])
        assert exc_info.value.op is op

    def test_drop_role_op(self):
        op = DropRoleOp(_role())
        with pytest.raises(DestructiveOperationError) as exc_info:
            filter_safe_ops([op])
        assert exc_info.value.op is op

    def test_revoke_privileges_op(self):
        op = _revoke_op()
        with pytest.raises(DestructiveOperationError) as exc_info:
            filter_safe_ops([op])
        assert exc_info.value.op is op

    def test_execute_sql_op(self):
        # ExecuteSQLOp is opaque — we cannot verify the SQL is safe.
        op = alembic_ops.ExecuteSQLOp("DROP TABLE s.t")
        with pytest.raises(DestructiveOperationError) as exc_info:
            filter_safe_ops([op])
        assert exc_info.value.op is op

    def test_modify_table_with_drop_column(self):
        drop = _drop_column_op()
        op = _modify_table_ops([drop])
        with pytest.raises(DestructiveOperationError) as exc_info:
            filter_safe_ops([op])
        # The inner op is reported, not the wrapper.
        assert exc_info.value.op is drop

    def test_modify_table_with_alter_column(self):
        alter = _alter_column_op()
        op = _modify_table_ops([alter])
        with pytest.raises(DestructiveOperationError) as exc_info:
            filter_safe_ops([op])
        assert exc_info.value.op is alter

    def test_modify_table_mixed_add_and_drop_raises(self):
        # Even if there is a safe inner op alongside a destructive one, raise.
        drop = _drop_column_op()
        op = _modify_table_ops([_add_column_op(), drop])
        with pytest.raises(DestructiveOperationError) as exc_info:
            filter_safe_ops([op])
        assert exc_info.value.op is drop

    def test_raises_on_first_blocked_op(self):
        # A safe op followed by a blocked one: the safe op must not be applied.
        safe = _create_table_op()
        blocked = alembic_ops.DropTableOp("other", schema="s")
        with pytest.raises(DestructiveOperationError):
            filter_safe_ops([safe, blocked])

    def test_unknown_op_type_is_blocked(self):
        # Closed-world assumption: unrecognised op types are never allowed.
        class _UnknownOp(alembic_ops.MigrateOperation):
            pass

        op = _UnknownOp()
        with pytest.raises(DestructiveOperationError) as exc_info:
            filter_safe_ops([op])
        assert exc_info.value.op is op


# ---------------------------------------------------------------------------
# Error attributes
# ---------------------------------------------------------------------------


class TestDestructiveOperationError:
    def test_error_stores_op(self):
        op = alembic_ops.DropTableOp("t", schema="s")
        with pytest.raises(DestructiveOperationError) as exc_info:
            filter_safe_ops([op])
        assert exc_info.value.op is op

    def test_error_message_contains_op_type_name(self):
        op = alembic_ops.DropTableOp("t", schema="s")
        with pytest.raises(DestructiveOperationError) as exc_info:
            filter_safe_ops([op])
        assert "DropTableOp" in str(exc_info.value)

    def test_inner_op_type_name_in_message_for_modify_table(self):
        alter = _alter_column_op()
        op = _modify_table_ops([alter])
        with pytest.raises(DestructiveOperationError) as exc_info:
            filter_safe_ops([op])
        assert "AlterColumnOp" in str(exc_info.value)
