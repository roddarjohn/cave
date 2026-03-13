"""Plugin that prevents direct DML on raw backing tables.

Direct INSERT/UPDATE/DELETE on raw backing tables bypasses the INSTEAD OF
triggers on the API views, which can corrupt dimension state (e.g. breaking
SCD Type 2 history in append-only dimensions, leaving orphaned EAV rows).

:class:`RawTableProtectionPlugin` installs BEFORE triggers on every raw
table it is given.  The triggers raise an exception if called outside a
trigger context (``pg_trigger_depth() = 0``), forcing all mutations to go
through the API view.  When a mutation arrives via the API view, the INSTEAD
OF trigger fires first (``pg_trigger_depth() = 1``), so the protection
triggers see depth > 0 and allow the operation.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy_declarative_extensions import (
    register_function,
    register_trigger,
)
from sqlalchemy_declarative_extensions.dialects.postgresql import (
    Function,
    Trigger,
)

from pgcraft.plugin import Plugin
from pgcraft.utils.naming import resolve_name

if TYPE_CHECKING:
    from sqlalchemy import MetaData, Table

    from pgcraft.factory.context import FactoryContext

_NAMING_DEFAULTS = {
    "protect_function": "_protect_%(schema)s_%(table_name)s",
    "protect_trigger": "_protect_%(schema)s_%(table_name)s_%(op)s",
}

_PROTECTION_FUNCTION_BODY = """\
BEGIN
    IF pg_trigger_depth() = 0 THEN
        RAISE EXCEPTION
            'Direct % on table "%.%" is not allowed. '
            'Mutate data through the API view instead.',
            TG_OP, TG_TABLE_SCHEMA, TG_TABLE_NAME;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;"""


def _register_table_protection(
    metadata: MetaData,
    table: Table,
    schema: str,
) -> None:
    """Install BEFORE INSERT/UPDATE/DELETE protection triggers on *table*.

    Registers one shared trigger function and three triggers (one per
    DML operation).  All objects are placed in *schema*.

    Args:
        metadata: SQLAlchemy ``MetaData`` to register on.
        table: The raw backing table to protect.
        schema: Schema that owns the table and will hold the function.

    """
    subs = {"schema": schema, "table_name": table.name}

    fn_name = resolve_name(metadata, "protect_function", subs, _NAMING_DEFAULTS)
    table_fullname = f"{schema}.{table.name}"

    register_function(
        metadata,
        Function(
            fn_name,
            _PROTECTION_FUNCTION_BODY,
            returns="trigger",
            language="plpgsql",
            schema=schema,
        ),
    )

    for op in ("insert", "update", "delete"):
        trigger_name = resolve_name(
            metadata,
            "protect_trigger",
            {**subs, "op": op},
            _NAMING_DEFAULTS,
        )
        register_trigger(
            metadata,
            Trigger.before(
                op,
                on=table_fullname,
                execute=f"{schema}.{fn_name}",
                name=trigger_name,
            ).for_each_row(),
        )


class RawTableProtectionPlugin(Plugin):
    """Prevent direct DML on raw backing tables.

    Installs BEFORE INSERT/UPDATE/DELETE triggers on every raw table
    specified by *table_keys*.  The triggers raise an exception when
    called at trigger depth 0 (i.e. directly, not from within another
    trigger), so mutations must go through the API view.

    All mutations through the API view arrive via an INSTEAD OF trigger
    at depth >= 1, which the protection triggers allow through.

    Args:
        *table_keys: One or more ``ctx`` keys whose values are the raw
            :class:`~sqlalchemy.Table` objects to protect.

    Example::

        RawTableProtectionPlugin("root_table", "attributes")

    """

    def __init__(self, *table_keys: str) -> None:
        """Store the ctx keys of raw tables to protect."""
        self.table_keys = list(table_keys)

    def resolved_requires(self) -> list[str]:
        """Return the ctx keys this plugin reads.

        Overrides the base implementation so that the topological sort
        correctly places this plugin after all table-creating plugins.
        """
        return list(self.table_keys)

    def run(self, ctx: FactoryContext) -> None:
        """Register protection triggers on each raw backing table."""
        for key in self.table_keys:
            table = ctx[key]
            _register_table_protection(ctx.metadata, table, ctx.schemaname)
