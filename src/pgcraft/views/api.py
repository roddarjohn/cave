"""APIView: PostgREST-facing view with auto-triggers."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import literal_column, select
from sqlalchemy.dialects import postgresql
from sqlalchemy_declarative_extensions import View, register_view

from pgcraft.plugins.protect import _register_table_protection
from pgcraft.resource import (
    APIResource,
    Grant,
    register_api_resource,
)
from pgcraft.utils.query import compile_query

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy import Table
    from sqlalchemy.sql.expression import Select

    from pgcraft.factory.base import ResourceFactory
    from pgcraft.factory.context import FactoryContext
    from pgcraft.statistics import JoinedView


def _build_join_columns(
    joins: dict[str, JoinedView],
    aliases: dict[str, str],
) -> list[Any]:
    """Build select-list entries for joined view columns."""
    result: list[Any] = []
    for name, view_info in joins.items():
        alias = aliases[name]
        result.extend(
            literal_column(f"{alias}.{col_name}").label(col_name)
            for col_name in view_info.column_names
        )
    return result


def _build_join_sql(  # noqa: PLR0913
    ctx: FactoryContext,
    primary: Table,
    select_cols: list[Any],
    joins: dict[str, JoinedView],
    aliases: dict[str, str],
    alias: str,
) -> str:
    """Build raw SQL with LEFT JOINs."""
    col_strs = []
    for col_expr in select_cols:
        compiled = col_expr.compile(dialect=postgresql.dialect())
        col_strs.append(str(compiled))

    table_ref = f"{ctx.schemaname}.{primary.name}"
    from_clause = f"{table_ref} AS {alias}"

    join_clauses = []
    pk_col_name = ctx["pk_columns"].first_key
    for name, view_info in joins.items():
        join_alias = aliases[name]
        join_clauses.append(
            f"LEFT OUTER JOIN {view_info.view_name}"
            f" AS {join_alias}"
            f" ON {alias}.{pk_col_name}"
            f" = {join_alias}.{view_info.join_key}"
        )

    parts = [
        "SELECT " + ", ".join(col_strs),
        f"\nFROM {from_clause}",
    ]
    parts.extend(f"\n{jc}" for jc in join_clauses)

    return "".join(parts)


def _resolve_columns(
    primary: Table,
    columns: list[str],
    alias: str = "p",
) -> list[Any]:
    """Build a select list from column names."""
    table_cols = {col.key for col in primary.columns}
    result: list[Any] = []
    for name in columns:
        if name in table_cols:
            result.append(literal_column(f"{alias}.{name}").label(name))
        else:
            msg = (
                f"Column {name!r} not found in table "
                f"columns ({sorted(table_cols)})"
            )
            raise ValueError(msg)
    return result


def _resolve_included_columns(
    primary: Table,
    columns: list[str] | None,
    exclude_columns: list[str] | None,
) -> list[str] | None:
    """Resolve the effective column list."""
    if columns is not None and exclude_columns is not None:
        msg = "Cannot specify both 'columns' and 'exclude_columns' on APIView"
        raise ValueError(msg)

    if exclude_columns is not None:
        table_cols = {col.key for col in primary.columns}
        for name in exclude_columns:
            if name not in table_cols:
                msg = (
                    f"Column {name!r} in exclude_columns "
                    f"not found on table "
                    f"({sorted(table_cols)})"
                )
                raise ValueError(msg)
        return [
            col.key for col in primary.columns if col.key not in exclude_columns
        ]

    return columns


def _build_view_definition(
    ctx: FactoryContext,
    primary: Table,
    columns: list[str] | None,
    exclude_columns: list[str] | None,
    query: Callable[[Select, Table], Select] | None,
) -> str:
    """Build the SQL definition for the API view."""
    # If user provides a query transform, use that directly.
    if query is not None:
        base_query = select(
            *[col.label(col.key) for col in primary.columns]
        ).select_from(primary)
        transformed = query(base_query, primary)
        return compile_query(transformed)

    joins: dict[str, JoinedView] = {}
    if "joins" in ctx:
        joins = ctx["joins"]

    effective_columns = _resolve_included_columns(
        primary, columns, exclude_columns
    )

    use_alias = effective_columns is not None or bool(joins)
    alias = "p" if use_alias else None

    select_cols = _build_select_cols(primary, effective_columns, alias)

    if joins:
        alias = alias or "p"
        aliases: dict[str, str] = {}
        for i, name in enumerate(joins):
            aliases[name] = f"s{i}" if i > 0 else "s"
        select_cols.extend(_build_join_columns(joins, aliases))
        return _build_join_sql(ctx, primary, select_cols, joins, aliases, alias)

    table_source = primary
    if alias is not None:
        table_source = primary.alias(alias)
    view_query = select(*select_cols).select_from(table_source)
    return compile_query(view_query)


def _build_select_cols(
    primary: Table,
    effective_columns: list[str] | None,
    alias: str | None,
) -> list[Any]:
    """Build the select column list.

    Args:
        primary: Source table.
        effective_columns: Explicit column list or ``None``.
        alias: Table alias to use, or ``None`` for no alias.

    """
    if effective_columns is not None and alias is not None:
        return _resolve_columns(primary, effective_columns, alias)
    if alias is not None:
        return [
            literal_column(f"{alias}.{col.key}").label(col.key)
            for col in primary.columns
        ]
    return [col.label(col.key) for col in primary.columns]


def _install_triggers(
    source: ResourceFactory,
    ctx: FactoryContext,
    columns: list[str] | None = None,
) -> None:
    """Auto-install triggers based on factory type.

    Args:
        source: The factory whose trigger class to use.
        ctx: The factory context.
        columns: Writable column names for the triggers.
            When ``None``, the trigger plugin uses all dim
            columns from ctx.

    """
    trigger_cls = getattr(source, "TRIGGER_PLUGIN_CLS", None)
    if trigger_cls is not None:
        import inspect  # noqa: PLC0415

        sig = inspect.signature(trigger_cls)
        if columns is not None and "columns" in sig.parameters:
            trigger_plugin = trigger_cls(columns=columns)
        else:
            trigger_plugin = trigger_cls()
        trigger_plugin.run(ctx)

    # Auto-install EAV check triggers on the API view.
    from pgcraft.factory.dimension.eav import (  # noqa: PLC0415
        PGCraftEAV,
    )
    from pgcraft.plugins.check import (  # noqa: PLC0415
        TriggerCheckPlugin,
    )

    if isinstance(source, PGCraftEAV):
        check_plugin = TriggerCheckPlugin(view_key="api")
        check_plugin.run(ctx)


def _install_protection(
    source: ResourceFactory,
    ctx: FactoryContext,
) -> None:
    """Auto-install raw table protection triggers."""
    protected_keys: list[str] = getattr(source, "PROTECTED_TABLE_KEYS", [])
    for key in protected_keys:
        if key in ctx:
            _register_table_protection(
                ctx.metadata,
                ctx[key],
                ctx.schemaname,
            )


class APIView:
    """Create a PostgREST-facing view with auto-selected triggers.

    Reads from a table factory's context to create an API view,
    INSTEAD OF triggers, and raw-table protection triggers.

    The trigger strategy is auto-selected based on the factory
    type (``source.TRIGGER_PLUGIN_CLS``).  Raw-table protection
    is auto-installed on all tables listed in
    ``source.PROTECTED_TABLE_KEYS``.

    Args:
        source: A :class:`~pgcraft.factory.base.ResourceFactory`
            instance whose table/context to expose.
        schema: Schema for the API view (default ``"api"``).
        grants: PostgREST privileges (default ``["select"]``).
        query: Optional callable ``(query, source_table) ->
            Select`` for SQLAlchemy-style view customization
            (joins, column filtering, etc.).
        protect_raw: Install BEFORE triggers blocking direct DML
            on raw backing tables (default ``True``).
        columns: Column names to include.  Mutually exclusive
            with ``exclude_columns``.
        exclude_columns: Column names to exclude.  Mutually
            exclusive with ``columns``.

    """

    def __init__(  # noqa: PLR0913
        self,
        source: ResourceFactory,
        schema: str = "api",
        grants: list[Grant] | None = None,
        query: Callable[[Select, Table], Select] | None = None,
        *,
        protect_raw: bool = True,
        columns: list[str] | None = None,
        exclude_columns: list[str] | None = None,
    ) -> None:
        """Create the API view, triggers, and protection."""
        self.source = source
        self.schema = schema
        self.grants: list[Grant] = grants if grants is not None else ["select"]
        ctx = source.ctx

        if "primary" not in ctx:
            msg = (
                "Source factory must produce a 'primary' "
                "context key for APIView."
            )
            raise ValueError(msg)
        primary = ctx["primary"]

        definition = _build_view_definition(
            ctx, primary, columns, exclude_columns, query
        )

        view = View(ctx.tablename, definition, schema=schema)
        register_view(ctx.metadata, view)
        self.view = view

        # Store the view in ctx so trigger plugins can find it.
        ctx.set("api", view, force=True)

        # Resolve which writable columns the view exposes.
        effective = _resolve_included_columns(primary, columns, exclude_columns)
        if effective is not None:
            dim_set = set(ctx.dim_column_names)
            writable = [c for c in effective if c in dim_set]
        else:
            writable = None

        register_api_resource(
            ctx.metadata,
            APIResource(
                name=ctx.tablename,
                schema=schema,
                grants=self.grants,
            ),
        )

        # Custom query= changes the view shape, so skip
        # auto-generated CRUD triggers (the view is read-only
        # or the caller manages writes externally).
        if query is None:
            _install_triggers(source, ctx, writable)

            if protect_raw:
                _install_protection(source, ctx)
