"""Plugin that generates PostgreSQL functions for ledger events."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy.dialects import postgresql as pg_dialect
from sqlalchemy_declarative_extensions import register_function
from sqlalchemy_declarative_extensions.dialects.postgresql import (
    Function,
    FunctionSecurity,
)

from pgcraft.errors import PGCraftValidationError
from pgcraft.ledger.events import (
    LedgerEvent,
    ParamCollector,
    _desired_table_ref,
    _input_table_ref,
)
from pgcraft.plugin import Dynamic, Plugin, requires
from pgcraft.utils.query import compile_query
from pgcraft.utils.template import load_template

if TYPE_CHECKING:
    from sqlalchemy import Column, Table

    from pgcraft.factory.context import FactoryContext

_TEMPLATES = Path(__file__).resolve().parent / "templates" / "ledger"


def _pg_type_str(col: Column) -> str:
    """Return the PostgreSQL type string for *col*.

    Args:
        col: A SQLAlchemy :class:`~sqlalchemy.Column`.

    Returns:
        Compiled PostgreSQL type string.

    """
    return col.type.compile(dialect=pg_dialect.dialect())


def _fn_name(schema: str, table: str, op: str) -> str:
    """Build a function name using the default naming convention.

    Args:
        schema: PostgreSQL schema name.
        table: Table name.
        op: Operation suffix.

    Returns:
        Unqualified function name (without schema prefix).

    """
    return f"{schema}_{table}_{op}"


def _validate_events(
    events: list[LedgerEvent],
    root_table: Table,
) -> None:
    """Validate all events against the root table columns.

    Args:
        events: List of events to validate.
        root_table: The ledger backing table.

    Raises:
        PGCraftValidationError: If any validation rule is violated.

    """
    names_seen: set[str] = set()

    for event in events:
        if event.name in names_seen:
            msg = (
                f"Duplicate event name {event.name!r}. "
                f"Event names must be unique within a ledger."
            )
            raise PGCraftValidationError(msg)
        names_seen.add(event.name)

        if event.desired is not None and not event.diff_keys:
            msg = (
                f"LedgerEvent {event.name!r}: "
                f"diff_keys is required when desired is set."
            )
            raise PGCraftValidationError(msg)

        if event.existing is not None and event.desired is None:
            msg = (
                f"LedgerEvent {event.name!r}: "
                f"existing requires desired to be set."
            )
            raise PGCraftValidationError(msg)

        col_names = {c.key for c in root_table.columns}
        for key in event.diff_keys:
            if key not in col_names:
                msg = (
                    f"LedgerEvent {event.name!r}: "
                    f"diff_key {key!r} is not a column on "
                    f"the ledger table.  Available columns: "
                    f"{sorted(col_names)}"
                )
                raise PGCraftValidationError(msg)


@requires("__root__", Dynamic("view_key"))
class LedgerActionsPlugin(Plugin):
    """Generate PostgreSQL functions for ledger events.

    Processes each :class:`~pgcraft.ledger.events.LedgerEvent` and
    registers a PostgreSQL function on ``ctx.metadata``.

    Must run **after** :class:`~pgcraft.plugins.ledger.LedgerTablePlugin`
    (needs ``"__root__"``) and
    :class:`~pgcraft.plugins.api.APIPlugin` (needs the API view key).

    Args:
        events: List of :class:`~pgcraft.ledger.events.LedgerEvent`
            instances to process.
        view_key: Context key for the API view (default ``"api"``).

    Raises:
        PGCraftValidationError: At run time if any event fails
            validation against the ledger schema.

    """

    def __init__(
        self,
        events: list[LedgerEvent],
        view_key: str = "api",
    ) -> None:
        """Store configuration."""
        self.events = list(events)
        self.view_key = view_key

    def run(self, ctx: FactoryContext) -> None:
        """Validate events and generate SQL functions.

        Args:
            ctx: The factory context.

        """
        root_table: Table = ctx["__root__"]
        api_view = ctx[self.view_key]
        api_view_fullname = f"{api_view.schema or 'api'}.{ctx.tablename}"
        backing_table = f"{ctx.schemaname}.{root_table.name}"

        _validate_events(self.events, root_table)

        for event in self.events:
            self._generate_event(
                event,
                ctx,
                root_table,
                api_view_fullname,
                backing_table,
            )

    def _generate_event(
        self,
        event: LedgerEvent,
        ctx: FactoryContext,
        root_table: Table,
        api_view_fullname: str,
        backing_table: str,
    ) -> None:
        """Generate the SQL function for a single event.

        Args:
            event: The :class:`LedgerEvent` to process.
            ctx: Factory context.
            root_table: Ledger backing table.
            api_view_fullname: Fully-qualified API view name.
            backing_table: Fully-qualified backing table name
                for direct inserts.

        """
        schema = ctx.schemaname

        # 1. Run input lambda with ParamCollector.
        collector = ParamCollector()
        input_select = event.input(collector)
        input_sql = compile_query(input_select)

        diff_mode = event.desired is not None
        template_ctx: dict = {
            "input_sql": input_sql,
            "api_view": api_view_fullname,
            "backing_table": backing_table,
            "diff_mode": diff_mode,
        }

        if diff_mode:
            # 2. Build input table ref and run desired.
            input_ref = _input_table_ref(input_select)
            desired_select = event.desired(input_ref)  # type: ignore[misc]
            desired_sql = compile_query(desired_select)

            # 3. Build desired table ref and run existing.
            desired_ref = _desired_table_ref(desired_select)

            if event.existing is not None:
                existing_select = event.existing(root_table, desired_ref)
                existing_sql = compile_query(existing_select)
            else:
                # No existing: empty select with same columns.
                existing_sql = (
                    "SELECT "
                    + ", ".join(
                        f"NULL::{_pg_type_str(root_table.c[k])}"
                        if k in {c.key for c in root_table.columns}
                        else "NULL"
                        for k in [
                            col.name
                            for col in (desired_select.selected_columns)
                        ]
                    )
                    + " WHERE false"
                )

            # 4. Compute column lists.
            desired_col_names = [
                col.name for col in desired_select.selected_columns
            ]
            all_cols = list(desired_col_names)

            # Existing cols: diff_keys + value, with NULL
            # padding for any extra columns (e.g. reason).
            existing_col_names = [
                col.name for col in existing_select.selected_columns
            ]
            existing_cols_padded: list[str] = []
            for col_name in all_cols:
                if col_name in existing_col_names:
                    existing_cols_padded.append(col_name)
                else:
                    existing_cols_padded.append(f"NULL AS {col_name}")

            # Passthrough cols: in desired but not diff_keys
            # or value.  These get MAX() in aggregation.
            passthrough_cols = [
                c
                for c in desired_col_names
                if c not in event.diff_keys and c != "value"
            ]

            template_ctx.update(
                desired_sql=desired_sql,
                existing_sql=existing_sql,
                all_cols=all_cols,
                desired_cols=", ".join(desired_col_names),
                existing_cols=", ".join(existing_cols_padded),
                diff_keys=list(event.diff_keys),
                passthrough_cols=passthrough_cols,
            )
        else:
            # Simple mode: columns from input select.
            all_cols = [col.name for col in input_select.selected_columns]
            template_ctx["all_cols"] = all_cols

        fn_body = load_template(_TEMPLATES / "event.sql.mako").render(
            **template_ctx
        )

        fn_name = _fn_name(schema, root_table.name, event.name)
        register_function(
            ctx.metadata,
            Function(
                fn_name,
                fn_body,
                returns=f"SETOF {backing_table}",
                language="sql",
                schema=schema,
                parameters=collector.function_params,
                security=FunctionSecurity.definer,
            ),
        )
