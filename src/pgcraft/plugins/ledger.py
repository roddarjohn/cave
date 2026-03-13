"""Plugins for ledger (append-only value) tables."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import (
    Column,
    Integer,
    Numeric,
    String,
    Table,
    func,
    select,
)
from sqlalchemy_declarative_extensions import (
    View,
    register_function,
    register_trigger,
    register_view,
)
from sqlalchemy_declarative_extensions.dialects.postgresql import (
    Function,
    FunctionSecurity,
    Trigger,
)

if TYPE_CHECKING:
    from pgcraft.factory.context import FactoryContext

from pgcraft.errors import PGCraftValidationError
from pgcraft.plugin import Dynamic, Plugin, produces, requires, singleton
from pgcraft.utils.naming import resolve_name
from pgcraft.utils.query import compile_query
from pgcraft.utils.template import load_template
from pgcraft.utils.trigger import register_view_triggers

_TEMPLATES = Path(__file__).resolve().parent / "templates" / "ledger"

_NAMING_DEFAULTS = {
    "ledger_function": "%(schema)s_%(table_name)s_%(op)s",
    "ledger_trigger": "%(schema)s_%(table_name)s_%(op)s",
}

_VALUE_TYPES = {
    "integer": Integer,
    "numeric": Numeric,
}


def _dim_column_names(ctx: FactoryContext) -> list[str]:
    """Extract writable (non-PK, non-computed) column names."""
    return [
        col.key
        for col in ctx.columns
        if not col.primary_key and not col.computed
    ]


@produces(Dynamic("table_key"), "__root__")
@requires("pk_columns", "entry_id_column", "created_at_column")
@singleton("__table__")
class LedgerTablePlugin(Plugin):
    """Create a ledger table with a value column.

    Combines ``ctx["pk_columns"]``, ``ctx.injected_columns``
    (provided by upstream plugins like ``UUIDEntryIDPlugin``,
    ``CreatedAtPlugin``, and ``DoubleEntryPlugin``), a ``value``
    column, and ``ctx.table_items`` (dimension columns) into a
    single append-only table.

    Args:
        value_type: Type for the value column. Must be
            ``"integer"`` or ``"numeric"`` (default ``"integer"``).
        table_key: Key under which the created table is stored
            in ``ctx`` (default ``"primary"``).

    Raises:
        PGCraftValidationError: If *value_type* is not a
            recognised type.

    """

    def __init__(
        self,
        value_type: str = "integer",
        table_key: str = "primary",
    ) -> None:
        """Store configuration."""
        if value_type not in _VALUE_TYPES:
            msg = (
                f"Unknown value_type {value_type!r}. "
                f"Must be one of: {sorted(_VALUE_TYPES)}"
            )
            raise PGCraftValidationError(msg)
        self.value_type = value_type
        self.table_key = table_key

    def run(self, ctx: FactoryContext) -> None:
        """Create the ledger table and store it in ctx."""
        pk_columns = ctx["pk_columns"]
        sa_type = _VALUE_TYPES[self.value_type]

        table = Table(
            ctx.tablename,
            ctx.metadata,
            *pk_columns,
            *ctx.injected_columns,
            Column("value", sa_type(), nullable=False),
            *ctx.table_items,
            schema=ctx.schemaname,
        )
        ctx[self.table_key] = table
        ctx["__root__"] = table


@requires(Dynamic("table_key"), Dynamic("view_key"), "entry_id_column")
class LedgerTriggerPlugin(Plugin):
    """Register an INSERT INSTEAD OF trigger on a ledger view.

    Only INSERT is supported -- ledger entries are immutable.
    UPDATE and DELETE on the API view will raise a PostgreSQL
    error naturally (no INSTEAD OF trigger defined).

    Args:
        table_key: Key in ``ctx`` for the backing table
            (default ``"primary"``).
        view_key: Key in ``ctx`` for the trigger target view
            (default ``"api"``).

    """

    def __init__(
        self,
        table_key: str = "primary",
        view_key: str = "api",
    ) -> None:
        """Store the context keys."""
        self.table_key = table_key
        self.view_key = view_key

    def run(self, ctx: FactoryContext) -> None:
        """Register INSTEAD OF INSERT trigger on the API view."""
        api_view = ctx[self.view_key]
        primary = ctx[self.table_key]
        base_fullname = f"{ctx.schemaname}.{primary.name}"
        entry_id_col = ctx["entry_id_column"]
        dim_cols = _dim_column_names(ctx)

        # Include entry_id and value alongside dimension columns.
        all_cols = [entry_id_col.name, "value", *dim_cols]
        template_vars = {
            "base_table": base_fullname,
            "cols": ", ".join(all_cols),
            "new_cols": ", ".join(f"NEW.{c}" for c in all_cols),
        }

        api_schema = api_view.schema or "api"
        register_view_triggers(
            metadata=ctx.metadata,
            view_schema=api_schema,
            view_fullname=f"{api_schema}.{ctx.tablename}",
            tablename=ctx.tablename,
            template_vars=template_vars,
            ops=[
                (
                    "insert",
                    load_template(_TEMPLATES / "insert.mako"),
                ),
            ],
            naming_defaults=_NAMING_DEFAULTS,
            function_key="ledger_function",
            trigger_key="ledger_trigger",
        )


_BALANCE_NAMING_DEFAULTS = {
    "ledger_balance_view": "%(table_name)s_balances",
}


@produces(Dynamic("balance_view_key"))
@requires(Dynamic("table_key"))
class LedgerBalanceViewPlugin(Plugin):
    """Create a view that shows current balances per dimension group.

    Generates ``SELECT dim_cols, SUM(value) AS balance FROM ledger
    GROUP BY dim_cols`` and registers it as a view.

    Args:
        dimensions: Column names to group by.  Must be a
            non-empty list of column names present on the
            ledger table.
        table_key: Key in ``ctx`` for the backing table
            (default ``"primary"``).
        balance_view_key: Key in ``ctx`` to store the balance
            view name under (default ``"balance_view"``).

    Raises:
        PGCraftValidationError: If *dimensions* is empty.

    """

    def __init__(
        self,
        dimensions: list[str],
        table_key: str = "primary",
        balance_view_key: str = "balance_view",
    ) -> None:
        """Store configuration."""
        if not dimensions:
            msg = "dimensions must be a non-empty list"
            raise PGCraftValidationError(msg)
        self.dimensions = list(dimensions)
        self.table_key = table_key
        self.balance_view_key = balance_view_key

    def run(self, ctx: FactoryContext) -> None:
        """Create and register the balance view."""
        table = ctx[self.table_key]
        dim_columns = [table.c[d] for d in self.dimensions]

        query = (
            select(
                *[c.label(c.key) for c in dim_columns],
                func.sum(table.c["value"]).label("balance"),
            )
            .select_from(table)
            .group_by(*dim_columns)
        )

        view_name = resolve_name(
            ctx.metadata,
            "ledger_balance_view",
            {
                "table_name": ctx.tablename,
                "schema": ctx.schemaname,
            },
            _BALANCE_NAMING_DEFAULTS,
        )

        register_view(
            ctx.metadata,
            View(
                view_name,
                compile_query(query),
                schema=ctx.schemaname,
            ),
        )
        ctx[self.balance_view_key] = view_name


_LATEST_NAMING_DEFAULTS = {
    "ledger_latest_view": "%(table_name)s_latest",
}


@produces(Dynamic("latest_view_key"))
@requires(Dynamic("table_key"), "created_at_column")
class LedgerLatestViewPlugin(Plugin):
    """Create a view showing the most recent row per dimension group.

    Uses PostgreSQL ``DISTINCT ON`` to select the latest row
    (by ``created_at``) for each unique combination of dimension
    values.  Useful for status-tracking ledgers where you care
    about current state, not historical sums.

    Args:
        dimensions: Column names to partition by.  Must be a
            non-empty list of column names present on the
            ledger table.
        table_key: Key in ``ctx`` for the backing table
            (default ``"primary"``).
        latest_view_key: Key in ``ctx`` to store the view name
            under (default ``"latest_view"``).

    Raises:
        PGCraftValidationError: If *dimensions* is empty.

    """

    def __init__(
        self,
        dimensions: list[str],
        table_key: str = "primary",
        latest_view_key: str = "latest_view",
    ) -> None:
        """Store configuration."""
        if not dimensions:
            msg = "dimensions must be a non-empty list"
            raise PGCraftValidationError(msg)
        self.dimensions = list(dimensions)
        self.table_key = table_key
        self.latest_view_key = latest_view_key

    def run(self, ctx: FactoryContext) -> None:
        """Create and register the latest-row view."""
        table = ctx[self.table_key]
        created_at_col = ctx["created_at_column"]
        dim_columns = [table.c[d] for d in self.dimensions]

        query = (
            select(table)
            .distinct(*dim_columns)
            .order_by(
                *dim_columns,
                table.c[created_at_col].desc(),
            )
        )

        view_name = resolve_name(
            ctx.metadata,
            "ledger_latest_view",
            {
                "table_name": ctx.tablename,
                "schema": ctx.schemaname,
            },
            _LATEST_NAMING_DEFAULTS,
        )

        register_view(
            ctx.metadata,
            View(
                view_name,
                compile_query(query),
                schema=ctx.schemaname,
            ),
        )
        ctx[self.latest_view_key] = view_name


_BALANCE_CHECK_NAMING_DEFAULTS = {
    "balance_check_function": ("%(schema)s_%(table_name)s_%(op)s"),
    "balance_check_trigger": ("%(schema)s_%(table_name)s_%(op)s"),
}


@requires(Dynamic("table_key"))
class LedgerBalanceCheckPlugin(Plugin):
    """Enforce a minimum balance per dimension group.

    Registers an ``AFTER INSERT FOR EACH STATEMENT`` trigger
    that checks ``SUM(value) >= min_balance`` for every
    dimension group affected by the inserted rows.  If any
    group violates the constraint the entire statement is
    rejected.

    Uses the same ``REFERENCING NEW TABLE`` transition-table
    pattern as
    :class:`~pgcraft.plugins.ledger.DoubleEntryTriggerPlugin`.

    Args:
        dimensions: Column names that define a balance group.
            Must be a non-empty list.
        min_balance: The minimum allowed ``SUM(value)`` per
            group (default ``0``).
        table_key: Key in ``ctx`` for the backing table
            (default ``"primary"``).

    Raises:
        PGCraftValidationError: If *dimensions* is empty.

    """

    def __init__(
        self,
        dimensions: list[str],
        min_balance: int = 0,
        table_key: str = "primary",
    ) -> None:
        """Store configuration."""
        if not dimensions:
            msg = "dimensions must be a non-empty list"
            raise PGCraftValidationError(msg)
        self.dimensions = list(dimensions)
        self.min_balance = min_balance
        self.table_key = table_key

    def run(self, ctx: FactoryContext) -> None:
        """Register the balance-check trigger."""
        table = ctx[self.table_key]
        table_fullname = f"{ctx.schemaname}.{table.name}"

        dim_cols = ", ".join(self.dimensions)
        dim_format = ", ".join("%" for _ in self.dimensions)
        dim_values = ", ".join(f"_bad.{d}" for d in self.dimensions)

        template = load_template(_TEMPLATES / "balance_check.mako")
        body = template.render(
            table=table_fullname,
            dim_cols=dim_cols,
            dim_format=dim_format,
            dim_values=dim_values,
            min_balance=self.min_balance,
        )

        fn_name = resolve_name(
            ctx.metadata,
            "balance_check_function",
            {
                "table_name": ctx.tablename,
                "schema": ctx.schemaname,
                "op": "balance_check",
            },
            _BALANCE_CHECK_NAMING_DEFAULTS,
        )
        trigger_name = resolve_name(
            ctx.metadata,
            "balance_check_trigger",
            {
                "table_name": ctx.tablename,
                "schema": ctx.schemaname,
                "op": "balance_check",
            },
            _BALANCE_CHECK_NAMING_DEFAULTS,
        )

        register_function(
            ctx.metadata,
            Function(
                fn_name,
                body,
                returns="trigger",
                language="plpgsql",
                schema=ctx.schemaname,
                security=FunctionSecurity.definer,
            ),
        )

        register_trigger(
            ctx.metadata,
            Trigger.after(
                "insert",
                on=table_fullname,
                execute=f"{ctx.schemaname}.{fn_name}",
                name=trigger_name,
            )
            .for_each_statement()
            .referencing_new_table_as("new_entries"),
        )


_DOUBLE_ENTRY_NAMING_DEFAULTS = {
    "double_entry_function": ("%(schema)s_%(table_name)s_%(op)s"),
    "double_entry_trigger": ("%(schema)s_%(table_name)s_%(op)s"),
}


@produces("double_entry_columns")
@singleton("__double_entry__")
class DoubleEntryPlugin(Plugin):
    """Add debit/credit semantics to a ledger table.

    Adds a ``direction`` column (``'debit'`` or ``'credit'``)
    to the schema items so that ``LedgerTablePlugin`` includes
    it in the table.  Also registers an AFTER INSERT constraint
    trigger that validates all rows sharing an ``entry_id``
    have equal total debits and credits.

    This plugin must appear **before** ``LedgerTablePlugin`` in
    the plugin list so its column is included in the table
    definition.

    Args:
        column_name: Name of the direction column
            (default ``"direction"``).

    """

    def __init__(
        self,
        column_name: str = "direction",
    ) -> None:
        """Store configuration."""
        self._column_name = column_name

    def run(self, ctx: FactoryContext) -> None:
        """Inject the direction column and store its name."""
        ctx.injected_columns.append(
            Column(
                self._column_name,
                String(6),
                nullable=False,
            ),
        )
        ctx["double_entry_columns"] = self._column_name


@requires(Dynamic("table_key"), "double_entry_columns", "entry_id_column")
class DoubleEntryTriggerPlugin(Plugin):
    """Register an AFTER INSERT trigger enforcing balanced entries.

    Validates that for every ``entry_id`` in the inserted batch,
    the sum of debit values equals the sum of credit values.
    Raises a PostgreSQL exception if any entry is unbalanced.

    Uses a statement-level trigger with a ``REFERENCING NEW TABLE``
    transition table so that multi-row inserts are checked as a
    whole, not row-by-row.

    Must run **after** ``LedgerTablePlugin`` (needs the table)
    and **after** ``DoubleEntryPlugin`` (needs column name).

    Args:
        table_key: Key in ``ctx`` for the backing table
            (default ``"primary"``).

    """

    def __init__(
        self,
        table_key: str = "primary",
    ) -> None:
        """Store the context key."""
        self.table_key = table_key

    def run(self, ctx: FactoryContext) -> None:
        """Register the constraint trigger on the ledger table."""
        table = ctx[self.table_key]
        direction_col = ctx["double_entry_columns"]
        entry_id_col = ctx["entry_id_column"]
        table_fullname = f"{ctx.schemaname}.{table.name}"

        template = load_template(_TEMPLATES / "double_entry_check.mako")
        body = template.render(
            table=table_fullname,
            direction_col=direction_col,
            entry_id_col=entry_id_col.name,
        )

        fn_name = resolve_name(
            ctx.metadata,
            "double_entry_function",
            {
                "table_name": ctx.tablename,
                "schema": ctx.schemaname,
                "op": "double_entry_check",
            },
            _DOUBLE_ENTRY_NAMING_DEFAULTS,
        )
        trigger_name = resolve_name(
            ctx.metadata,
            "double_entry_trigger",
            {
                "table_name": ctx.tablename,
                "schema": ctx.schemaname,
                "op": "double_entry_check",
            },
            _DOUBLE_ENTRY_NAMING_DEFAULTS,
        )

        register_function(
            ctx.metadata,
            Function(
                fn_name,
                body,
                returns="trigger",
                language="plpgsql",
                schema=ctx.schemaname,
                security=FunctionSecurity.definer,
            ),
        )

        register_trigger(
            ctx.metadata,
            Trigger.after(
                "insert",
                on=table_fullname,
                execute=f"{ctx.schemaname}.{fn_name}",
                name=trigger_name,
            )
            .for_each_statement()
            .referencing_new_table_as("new_entries"),
        )
