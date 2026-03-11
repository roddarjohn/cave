"""Custom alembic renderers that format SQL with pglast."""

import re
from textwrap import indent
from typing import Any

import pglast
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
# Margin at which pglast keeps comma-separated lists on one line.
_COMPACT_LISTS_MARGIN = 80

# Regex matching a ``$$ ... $$`` function body in prettified output.
_BODY_RE = re.compile(r"(AS \$\$)(.*?)(\$\$)", re.DOTALL)


# ---------------------------------------------------------------------------
# SQL formatting
# ---------------------------------------------------------------------------


def _prettify(sql: str) -> str:
    """Format a SQL statement using pglast."""
    return pglast.prettify(sql, compact_lists_margin=_COMPACT_LISTS_MARGIN)


def _format_function_body(sql: str) -> str:
    """Format a CREATE FUNCTION, indenting the PL/pgSQL body."""
    formatted = _prettify(sql)
    m = _BODY_RE.search(formatted)
    if not m:
        return formatted

    body = m.group(2)
    lines = body.strip().splitlines()
    indented: list[str] = []
    for line in lines:
        stripped = line.strip()
        if stripped in ("BEGIN", "END;"):
            indented.append(stripped)
        else:
            # Preserve relative indentation within the body.
            leading = len(line) - len(line.lstrip())
            indented.append("    " + " " * leading + stripped)
    new_body = "\n".join(indented)
    return (
        formatted[: m.start()]
        + "AS $$\n"
        + new_body
        + "\n$$"
        + formatted[m.end() :]
    )


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
    """Render ops whose SQL pglast can format (views, etc.)."""
    assert autogen_context.connection  # noqa: S101
    commands = op.to_sql(autogen_context.connection.dialect)
    return [_render_execute(_prettify(cmd)) for cmd in commands]


def _render_ddl_op(
    _autogen_context: AutogenContext,
    op: Any,  # noqa: ANN401
) -> list[str]:
    """Render function/procedure ops with formatted bodies."""
    commands = op.to_sql()
    return [_render_execute(_format_function_body(cmd)) for cmd in commands]


def _render_trigger(
    autogen_context: AutogenContext,
    op: Any,  # noqa: ANN401
) -> list[str]:
    """Render trigger ops."""
    assert autogen_context.connection  # noqa: S101
    commands = op.to_sql(autogen_context.connection)
    return [_render_execute(_prettify(cmd)) for cmd in commands]


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
        else _render_execute(_prettify(str(cmd)))
        for cmd in statements
    ]


def _render_role(
    autogen_context: AutogenContext,
    op: Any,  # noqa: ANN401
) -> list[str]:
    """Render a role op with pglast-formatted SQL."""
    is_dynamic = op.role.is_dynamic
    if is_dynamic:
        autogen_context.imports.add("import os")
    return [
        _render_execute(_prettify(cmd), fstring=is_dynamic)
        for cmd in op.to_sql(raw=False)
    ]


def _render_grant(
    _autogen_context: AutogenContext,
    op: Any,  # noqa: ANN401
) -> str:
    """Render a grant/revoke with pglast-formatted SQL."""
    return _render_execute_text(_prettify(str(op.to_sql())))


_RENDERER_MAP: dict[type, Any] = {
    CreateViewOp: _render_sql_op,
    UpdateViewOp: _render_sql_op,
    DropViewOp: _render_sql_op,
    CreateFunctionOp: _render_ddl_op,
    UpdateFunctionOp: _render_ddl_op,
    DropFunctionOp: _render_ddl_op,
    CreateProcedureOp: _render_ddl_op,
    UpdateProcedureOp: _render_ddl_op,
    DropProcedureOp: _render_ddl_op,
    CreateTriggerOp: _render_trigger,
    UpdateTriggerOp: _render_trigger,
    DropTriggerOp: _render_trigger,
    CreateSchemaOp: _render_schema,
    DropSchemaOp: _render_schema,
    CreateRoleOp: _render_role,
    UpdateRoleOp: _render_role,
    DropRoleOp: _render_role,
    GrantPrivilegesOp: _render_grant,
    RevokePrivilegesOp: _render_grant,
}


def register_renderers() -> None:
    """Override the library's renderers with pglast-formatted versions."""
    for op_type, renderer in _RENDERER_MAP.items():
        renderers.dispatch_for(op_type, replace=True)(renderer)
