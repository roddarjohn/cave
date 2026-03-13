"""Shared trigger registration for dimension factories."""

from mako.template import Template
from sqlalchemy import MetaData
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
