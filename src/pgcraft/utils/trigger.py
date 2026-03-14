"""Shared trigger registration for dimension factories."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy_declarative_extensions import (
    register_function,
    register_trigger,
)
from sqlalchemy_declarative_extensions.dialects.postgresql import (
    Function,
    FunctionSecurity,
    Trigger,
)

from pgcraft.utils.naming import resolve_name

if TYPE_CHECKING:
    from mako.template import Template
    from sqlalchemy import MetaData

    from pgcraft.factory.context import FactoryContext


def collect_trigger_views(
    ctx: FactoryContext,
    view_key: str,
) -> list[tuple[str, str]]:
    """Return ``(schema, fullname)`` pairs for all views to trigger on.

    Always includes the private dimension view in ``ctx.schemaname``.
    If ``view_key`` is present in ``ctx`` (e.g. the API view created
    by ``APIPlugin``), that view's schema and fullname are appended.

    Args:
        ctx: The active factory context.
        view_key: Key under which an additional trigger target
            (e.g. ``"api"``) may be stored in ``ctx``.

    Returns:
        List of ``(schema, fully_qualified_name)`` tuples.

    """
    views: list[tuple[str, str]] = [
        (ctx.schemaname, f"{ctx.schemaname}.{ctx.tablename}"),
    ]
    if view_key in ctx:
        api_view = ctx[view_key]
        api_schema = api_view.schema or "api"
        views.append((api_schema, f"{api_schema}.{ctx.tablename}"))
    return views


def register_view_triggers(  # noqa: PLR0913
    metadata: MetaData,
    *,
    view_schema: str,
    view_fullname: str,
    tablename: str,
    template_vars: dict,
    ops: list[tuple[str, Template]],
    naming_defaults: dict[str, str],
    function_key: str,
    trigger_key: str,
) -> None:
    """Register INSTEAD OF triggers for a single view.

    :param metadata: SQLAlchemy ``MetaData`` to register on.
    :param view_schema: Schema the view lives in.
    :param view_fullname: Fully qualified view name.
    :param tablename: Base dimension table name.
    :param template_vars: Variables passed to Mako templates.
    :param ops: List of ``(operation, template)`` tuples.
    :param naming_defaults: Default naming templates.
    :param function_key: Key for function name resolution.
    :param trigger_key: Key for trigger name resolution.
    """
    subs = {"table_name": tablename, "schema": view_schema}

    for op, template in ops:
        fn_name = resolve_name(
            metadata,
            function_key,
            {**subs, "op": op},
            naming_defaults,
        )
        trigger_name = resolve_name(
            metadata,
            trigger_key,
            {**subs, "op": op},
            naming_defaults,
        )

        register_function(
            metadata,
            Function(
                fn_name,
                template.render(**template_vars),
                returns="trigger",
                language="plpgsql",
                schema=view_schema,
                security=FunctionSecurity.definer,
            ),
        )

        register_trigger(
            metadata,
            Trigger.instead_of(
                op,
                on=view_fullname,
                execute=f"{view_schema}.{fn_name}",
                name=trigger_name,
            ).for_each_row(),
        )
