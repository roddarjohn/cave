"""Custom alembic renderers that format SQL with sqlglot."""

from textwrap import indent
from typing import Any

import sqlglot
from alembic.autogenerate.api import AutogenContext
from alembic.autogenerate.render import renderers
from sqlalchemy.sql.ddl import CreateSchema, DropSchema
from sqlalchemy_declarative_extensions.function.compare import (
    CreateFunctionOp,
    DropFunctionOp,
    UpdateFunctionOp,
)
from sqlalchemy_declarative_extensions.grant.compare import (
    GrantPrivilegesOp,
    RevokePrivilegesOp,
)
from sqlalchemy_declarative_extensions.procedure.compare import (
    CreateProcedureOp,
    DropProcedureOp,
    UpdateProcedureOp,
)
from sqlalchemy_declarative_extensions.role.compare import (
    CreateRoleOp,
    DropRoleOp,
    UpdateRoleOp,
)
from sqlalchemy_declarative_extensions.schema.compare import (
    CreateSchemaOp,
    DropSchemaOp,
    SchemaOp,
)
from sqlalchemy_declarative_extensions.trigger.compare import (
    CreateTriggerOp,
    DropTriggerOp,
    UpdateTriggerOp,
)
from sqlalchemy_declarative_extensions.view.compare import (
    CreateViewOp,
    DropViewOp,
    UpdateViewOp,
)

# Maximum line width for inline SQL before forcing multi-line.
_MAX_LINE = 80
# Width budget for SQL content inside an indented triple-quoted block.
_WRAP_WIDTH = 68


# ---------------------------------------------------------------------------
# SQL formatting
# ---------------------------------------------------------------------------


def _format_sql(sql: str) -> str:
    """Format a SQL query using sqlglot (SELECT, CREATE VIEW, etc.)."""
    return sqlglot.transpile(
        sql, read="postgres", write="postgres", pretty=True
    )[0]


def _wrap_sql(sql: str) -> str:
    """Word-wrap SQL that sqlglot can't parse (GRANT, CREATE ROLE, etc.)."""
    if "\n" in sql or len(sql) <= _WRAP_WIDTH:
        return sql
    words = sql.split()
    lines: list[str] = [words[0]]
    for word in words[1:]:
        if len(lines[-1]) + 1 + len(word) <= _WRAP_WIDTH:
            lines[-1] += " " + word
        else:
            lines.append(word)
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Code generation helpers
# ---------------------------------------------------------------------------


def _render_execute(sql: str, *, fstring: bool = False) -> str:
    """Render ``op.execute(\"\"\"...\"\"\")``, going multi-line when needed."""
    prefix = "f" if fstring else ""
    inline = f'op.execute({prefix}"""{sql}""")'
    if "\n" in sql or sql.endswith('"') or len(inline) > _MAX_LINE:
        return f'op.execute({prefix}"""\n{indent(sql, "    ")}\n""")'
    return inline


def _render_execute_text(sql: str) -> str:
    """Render ``op.execute(sa.text(\"\"\"...\"\"\"))``, always multi-line."""
    return (
        f'op.execute(\n    sa.text("""\n{indent(sql, "        ")}\n    """)\n)'
    )


# ---------------------------------------------------------------------------
# Per-op-type renderers
# ---------------------------------------------------------------------------


def _render_sql_op(
    autogen_context: AutogenContext,
    op: Any,  # noqa: ANN401
) -> list[str]:
    """Render ops whose SQL sqlglot can format (views, functions, etc.)."""
    if isinstance(op, (CreateViewOp, DropViewOp, UpdateViewOp)):
        assert autogen_context.connection  # noqa: S101
        commands = op.to_sql(autogen_context.connection.dialect)
    elif isinstance(op, (CreateTriggerOp, DropTriggerOp, UpdateTriggerOp)):
        assert autogen_context.connection  # noqa: S101
        commands = op.to_sql(autogen_context.connection)
    else:
        commands = op.to_sql()
    return [_render_execute(_format_sql(cmd)) for cmd in commands]


def _render_schema(
    autogen_context: AutogenContext,
    op: SchemaOp,
) -> list[str]:
    """Render a schema op, using DDL objects where possible."""
    statements = op.to_sql()
    cls_names = {
        s.__class__.__name__
        for s in statements
        if isinstance(s, (CreateSchema, DropSchema))
    }
    if cls_names:
        autogen_context.imports.add(
            f"from sqlalchemy.sql.ddl import {', '.join(cls_names)}"
        )
    return [
        f'op.execute({cmd.__class__.__name__}("{cmd.element}"))'
        if isinstance(cmd, (CreateSchema, DropSchema))
        else _render_execute(_format_sql(str(cmd)))
        for cmd in statements
    ]


def _render_role(
    autogen_context: AutogenContext,
    op: Any,  # noqa: ANN401
) -> list[str]:
    """Render a role op with word-wrapped SQL (sqlglot can't parse these)."""
    is_dynamic = op.role.is_dynamic
    if is_dynamic:
        autogen_context.imports.add("import os")
    return [
        _render_execute(_wrap_sql(cmd), fstring=is_dynamic)
        for cmd in op.to_sql(raw=False)
    ]


def _render_grant(
    _autogen_context: AutogenContext,
    op: Any,  # noqa: ANN401
) -> str:
    """Render a grant/revoke with word-wrapped SQL."""
    return _render_execute_text(_wrap_sql(str(op.to_sql())))


_RENDERER_MAP: dict[type, Any] = {
    CreateViewOp: _render_sql_op,
    UpdateViewOp: _render_sql_op,
    DropViewOp: _render_sql_op,
    CreateFunctionOp: _render_sql_op,
    UpdateFunctionOp: _render_sql_op,
    DropFunctionOp: _render_sql_op,
    CreateProcedureOp: _render_sql_op,
    UpdateProcedureOp: _render_sql_op,
    DropProcedureOp: _render_sql_op,
    CreateTriggerOp: _render_sql_op,
    UpdateTriggerOp: _render_sql_op,
    DropTriggerOp: _render_sql_op,
    CreateSchemaOp: _render_schema,
    DropSchemaOp: _render_schema,
    CreateRoleOp: _render_role,
    UpdateRoleOp: _render_role,
    DropRoleOp: _render_role,
    GrantPrivilegesOp: _render_grant,
    RevokePrivilegesOp: _render_grant,
}


def register_renderers() -> None:
    """Override the library's renderers with sqlglot-formatted versions."""
    for op_type, renderer in _RENDERER_MAP.items():
        renderers.dispatch_for(op_type, replace=True)(renderer)
