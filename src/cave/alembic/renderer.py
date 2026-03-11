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
from sqlalchemy_declarative_extensions.procedure.compare import (
    CreateProcedureOp,
    DropProcedureOp,
    UpdateProcedureOp,
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


def _format_sql(sql: str) -> str:
    """Format a SQL string using sqlglot."""
    return sqlglot.transpile(
        sql, read="postgres", write="postgres", pretty=True
    )[0]


def _render_command(command: str) -> str:
    """Render a single SQL command as an ``op.execute(...)`` call."""
    formatted = _format_sql(command)
    if "\n" in formatted:
        indented = indent(formatted, "    ")
        return f'op.execute("""\n{indented}\n""")'
    return f'op.execute("""{formatted}""")'


def _render_view(
    autogen_context: AutogenContext,
    op: Any,  # noqa: ANN401
) -> list[str]:
    """Render a view op with sqlglot-formatted SQL."""
    assert autogen_context.connection  # noqa: S101
    dialect = autogen_context.connection.dialect
    commands = op.to_sql(dialect)
    return [_render_command(command) for command in commands]


def _render_function(
    _autogen_context: AutogenContext,
    op: Any,  # noqa: ANN401
) -> list[str]:
    """Render a function op with sqlglot-formatted SQL."""
    commands = op.to_sql()
    return [_render_command(command) for command in commands]


def _render_procedure(
    _autogen_context: AutogenContext,
    op: Any,  # noqa: ANN401
) -> list[str]:
    """Render a procedure op with sqlglot-formatted SQL."""
    commands = op.to_sql()
    return [_render_command(command) for command in commands]


def _render_trigger(
    autogen_context: AutogenContext,
    op: Any,  # noqa: ANN401
) -> list[str]:
    """Render a trigger op with sqlglot-formatted SQL."""
    assert autogen_context.connection  # noqa: S101
    commands = op.to_sql(autogen_context.connection)
    return [_render_command(command) for command in commands]


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
        (f'op.execute({command.__class__.__name__}("{command.element}"))')
        if isinstance(command, (CreateSchema, DropSchema))
        else _render_command(str(command))
        for command in statements
    ]


_RENDERER_MAP: dict[type, Any] = {
    CreateViewOp: _render_view,
    UpdateViewOp: _render_view,
    DropViewOp: _render_view,
    CreateFunctionOp: _render_function,
    UpdateFunctionOp: _render_function,
    DropFunctionOp: _render_function,
    CreateProcedureOp: _render_procedure,
    UpdateProcedureOp: _render_procedure,
    DropProcedureOp: _render_procedure,
    CreateTriggerOp: _render_trigger,
    UpdateTriggerOp: _render_trigger,
    DropTriggerOp: _render_trigger,
    CreateSchemaOp: _render_schema,
    DropSchemaOp: _render_schema,
}


def register_renderers() -> None:
    """Override the library's renderers with sqlglot-formatted versions."""
    for op_type, renderer in _RENDERER_MAP.items():
        renderers.dispatch_for(op_type, replace=True)(renderer)
