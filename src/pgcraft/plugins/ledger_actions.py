"""Plugin that generates PostgreSQL functions for ledger actions."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import Column, MetaData, Table
from sqlalchemy.dialects import postgresql as pg_dialect
from sqlalchemy_declarative_extensions import register_function
from sqlalchemy_declarative_extensions.dialects.postgresql import (
    Function,
    FunctionParam,
    FunctionSecurity,
)

from pgcraft.config import PGCraftConfig
from pgcraft.errors import PGCraftValidationError
from pgcraft.ledger.actions import Action, EventAction, StateAction
from pgcraft.plugin import Dynamic, Plugin, requires
from pgcraft.utils.pgcraft_schema import _ensure_pgcraft_utilities
from pgcraft.utils.template import load_template

if TYPE_CHECKING:
    from pgcraft.factory.context import FactoryContext

_TEMPLATES = Path(__file__).resolve().parent / "templates" / "ledger"


def _pg_type_str(col: Column) -> str:
    """Return the PostgreSQL type string for *col* (e.g. ``"INTEGER"``).

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


def _validate_state_action(
    action: StateAction,
    col_names: set[str],
) -> None:
    """Validate a single StateAction.

    Args:
        action: The action to validate.
        col_names: Set of column names on the ledger table.

    Raises:
        PGCraftValidationError: If any validation rule is violated.

    """
    if not action.diff_keys:
        msg = (
            f"StateAction {action.name!r}: diff_keys must be a non-empty list."
        )
        raise PGCraftValidationError(msg)

    diff_set = set(action.diff_keys)
    for key in action.diff_keys:
        if key not in col_names:
            msg = (
                f"StateAction {action.name!r}: "
                f"diff_key {key!r} is not a column on the "
                f"ledger table.  Available columns: "
                f"{sorted(col_names)}"
            )
            raise PGCraftValidationError(msg)

    for key in action.write_only_keys:
        if key not in col_names:
            msg = (
                f"StateAction {action.name!r}: "
                f"write_only_key {key!r} is not a column on the "
                f"ledger table.  Available columns: "
                f"{sorted(col_names)}"
            )
            raise PGCraftValidationError(msg)
        if key in diff_set:
            msg = (
                f"StateAction {action.name!r}: "
                f"write_only_key {key!r} overlaps with diff_keys. "
                f"Keys must not appear in both."
            )
            raise PGCraftValidationError(msg)
        if key == "value":
            msg = (
                f"StateAction {action.name!r}: "
                f"write_only_key 'value' is not allowed — "
                f"'value' is the delta column."
            )
            raise PGCraftValidationError(msg)


def _validate_actions(
    actions: list[Action],
    root_table: Table,
) -> None:
    """Validate all actions against the root table columns.

    Args:
        actions: List of actions to validate.
        root_table: The ledger backing table (``ctx["__root__"]``).

    Raises:
        PGCraftValidationError: If any validation rule is violated.

    """
    col_names = {c.key for c in root_table.columns}
    names_seen: set[str] = set()

    for action in actions:
        if action.name in names_seen:
            msg = (
                f"Duplicate action name {action.name!r}. "
                f"Action names must be unique within a ledger."
            )
            raise PGCraftValidationError(msg)
        names_seen.add(action.name)

        if isinstance(action, StateAction):
            _validate_state_action(action, col_names)
        else:
            for key in action.write_only_keys:
                if key not in col_names:
                    msg = (
                        f"Action {action.name!r}: "
                        f"write_only_key {key!r} is not a column on "
                        f"the ledger table.  Available columns: "
                        f"{sorted(col_names)}"
                    )
                    raise PGCraftValidationError(msg)


def _resolve_event_dim_keys(
    action: EventAction,
    actions: list[Action],
    ctx: FactoryContext,
) -> list[str]:
    """Resolve the dimension keys for an EventAction.

    Priority order:

    1. ``action.dim_keys`` if explicitly set.
    2. ``diff_keys`` from a sibling :class:`StateAction`.
    3. All writable (non-PK, non-computed) dimension columns.

    Args:
        action: The EventAction being processed.
        actions: All actions for the ledger (to find siblings).
        ctx: The factory context (for column inspection).

    Returns:
        List of dimension column names.

    """
    if action.dim_keys is not None:
        return list(action.dim_keys)

    for a in actions:
        if isinstance(a, StateAction):
            return list(a.diff_keys)

    return [
        col.key
        for col in ctx.columns
        if not col.primary_key and not col.computed
    ]


@requires("__root__", Dynamic("view_key"))
class LedgerActionsPlugin(Plugin):
    """Generate PostgreSQL functions for ledger actions.

    Processes each :class:`~pgcraft.ledger.actions.Action` in the
    provided list and registers the corresponding PostgreSQL functions
    on ``ctx.metadata``.

    Also ensures the pgcraft utility schema and
    ``ledger_apply_state`` function are registered (idempotent).

    Must run **after** :class:`~pgcraft.plugins.ledger.LedgerTablePlugin`
    (needs ``"__root__"``) and
    :class:`~pgcraft.plugins.api.APIPlugin` (needs the API view key).

    Args:
        actions: List of :class:`~pgcraft.ledger.actions.Action`
            instances to process.
        view_key: Context key for the API view (default ``"api"``).

    Raises:
        PGCraftValidationError: At run time if any action fails
            validation against the ledger schema.

    """

    def __init__(
        self,
        actions: list[Action],
        view_key: str = "api",
    ) -> None:
        """Store configuration."""
        self.actions = list(actions)
        self.view_key = view_key

    def run(self, ctx: FactoryContext) -> None:
        """Validate actions and generate SQL functions.

        Args:
            ctx: The factory context.

        """
        root_table: Table = ctx["__root__"]
        api_view = ctx[self.view_key]
        api_view_fullname = f"{api_view.schema or 'api'}.{ctx.tablename}"
        table_fullname = f"{ctx.schemaname}.{root_table.name}"

        config: PGCraftConfig = ctx.metadata.info.get(
            "pgcraft_config", PGCraftConfig()
        )
        utility_schema = config.utility_schema

        _validate_actions(self.actions, root_table)
        _ensure_pgcraft_utilities(ctx.metadata, utility_schema)

        value_pg_type = _pg_type_str(root_table.c["value"])

        for action in self.actions:
            if isinstance(action, StateAction):
                self._generate_state_action(
                    action,
                    ctx,
                    root_table,
                    table_fullname,
                    api_view_fullname,
                    value_pg_type,
                    utility_schema,
                )
            elif isinstance(action, EventAction):
                self._generate_event_action(
                    action,
                    ctx,
                    root_table,
                    api_view_fullname,
                    value_pg_type,
                )

    def _generate_state_action(  # noqa: PLR0913
        self,
        action: StateAction,
        ctx: FactoryContext,
        root_table: Table,
        table_fullname: str,
        api_view_fullname: str,
        value_pg_type: str,
        utility_schema: str,
    ) -> None:
        """Generate begin and apply functions for *action*.

        Args:
            action: The :class:`~pgcraft.ledger.actions.StateAction`.
            ctx: Factory context.
            root_table: Ledger backing table.
            table_fullname: Fully-qualified backing table name.
            api_view_fullname: Fully-qualified API view name.
            value_pg_type: PG type string for the value column.
            utility_schema: Schema name for utility functions.

        """
        staging_name = f"_{root_table.name}_{action.name}"
        schema = ctx.schemaname
        table = root_table.name

        begin_fn_name = _fn_name(schema, table, action.name + "_begin")
        apply_fn_name = _fn_name(schema, table, action.name + "_apply")

        # Build column definitions for the staging temp table.
        col_defs: list[str] = [
            f"{key} {_pg_type_str(root_table.c[key])}"
            + ("" if root_table.c[key].nullable else " NOT NULL")
            for key in action.diff_keys
        ]
        col_defs.append(f"value {value_pg_type} NOT NULL")

        # Build the SQLAlchemy Table for the staging temp table.
        # Uses an isolated MetaData so it is never included in migrations.
        staging_meta = MetaData()
        staging_cols: list[Column] = [
            Column(key, type(root_table.c[key].type)())
            for key in action.diff_keys
        ]
        staging_cols.append(Column("value", type(root_table.c["value"].type)()))
        staging_table_obj = Table(staging_name, staging_meta, *staging_cols)

        # --- begin function ---
        begin_body = load_template(_TEMPLATES / "state_begin.mako").render(
            staging_table=staging_name,
            col_defs=col_defs,
        )
        register_function(
            ctx.metadata,
            Function(
                begin_fn_name,
                begin_body,
                returns="void",
                language="plpgsql",
                schema=schema,
                security=FunctionSecurity.definer,
            ),
        )

        # --- apply function ---
        write_only_cols = list(action.write_only_keys)
        apply_params: list[FunctionParam] = [
            FunctionParam.input(
                f"p_{k}",
                _pg_type_str(root_table.c[k]),
                default="NULL",
            )
            for k in write_only_cols
        ]
        apply_params.append(FunctionParam.table("delta", "BIGINT"))

        apply_body = load_template(_TEMPLATES / "state_apply.mako").render(
            table=table_fullname,
            api_view=api_view_fullname,
            staging_table=staging_name,
            diff_keys=action.diff_keys,
            write_only_cols=write_only_cols,
            partial=action.partial,
            utility_schema=utility_schema,
        )
        register_function(
            ctx.metadata,
            Function(
                apply_fn_name,
                apply_body,
                returns="TABLE(delta BIGINT)",
                language="plpgsql",
                schema=schema,
                parameters=apply_params,
                security=FunctionSecurity.definer,
            ),
        )

        # Populate private attrs used by LedgerStateRecorder.
        action._staging_table = staging_table_obj  # noqa: SLF001
        action._begin_fn = f"{schema}.{begin_fn_name}"  # noqa: SLF001
        action._apply_fn = f"{schema}.{apply_fn_name}"  # noqa: SLF001

    def _generate_event_action(
        self,
        action: EventAction,
        ctx: FactoryContext,
        root_table: Table,
        api_view_fullname: str,
        value_pg_type: str,
    ) -> None:
        """Generate the record function for *action*.

        Args:
            action: The :class:`~pgcraft.ledger.actions.EventAction`.
            ctx: Factory context.
            root_table: Ledger backing table.
            api_view_fullname: Fully-qualified API view name.
            value_pg_type: PG type string for the value column.

        """
        schema = ctx.schemaname
        table = root_table.name

        dim_keys = _resolve_event_dim_keys(action, self.actions, ctx)
        write_only_cols = list(action.write_only_keys)

        event_params: list[FunctionParam] = [
            FunctionParam.input("p_value", value_pg_type),
            *[
                FunctionParam.input(f"p_{k}", _pg_type_str(root_table.c[k]))
                for k in dim_keys
            ],
            *[
                FunctionParam.input(
                    f"p_{k}",
                    _pg_type_str(root_table.c[k]),
                    default="NULL",
                )
                for k in write_only_cols
            ],
        ]

        record_body = load_template(_TEMPLATES / "event_record.mako").render(
            api_view=api_view_fullname,
            dim_keys=dim_keys,
            write_only_cols=write_only_cols,
        )
        fn_name = _fn_name(schema, table, action.name)
        register_function(
            ctx.metadata,
            Function(
                fn_name,
                record_body,
                returns="void",
                language="plpgsql",
                schema=schema,
                parameters=event_params,
                security=FunctionSecurity.definer,
            ),
        )

        action._record_fn = f"{schema}.{fn_name}"  # noqa: SLF001
