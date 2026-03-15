"""APIView: PostgREST-facing view with auto-triggers."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import literal_column, select
from sqlalchemy.dialects import postgresql
from sqlalchemy_declarative_extensions import View, register_view

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
    permitted_operations: list[str] | None = None,
) -> None:
    """Auto-install triggers based on factory type.

    Args:
        source: The factory whose trigger class to use.
        ctx: The factory context.
        columns: Writable column names for the triggers.
            When ``None``, the trigger plugin uses all dim
            columns from ctx.
        permitted_operations: Which DML operations get
            INSTEAD OF triggers.  When ``None``, creates
            all supported triggers.

    """
    trigger_cls = getattr(source, "TRIGGER_PLUGIN_CLS", None)
    if trigger_cls is not None:
        import inspect  # noqa: PLC0415

        sig = inspect.signature(trigger_cls)
        kwargs: dict[str, Any] = {}
        if columns is not None and "columns" in sig.parameters:
            kwargs["columns"] = columns
        if (
            permitted_operations is not None
            and "permitted_operations" in sig.parameters
        ):
            kwargs["permitted_operations"] = permitted_operations
        trigger_plugin = trigger_cls(**kwargs)
        trigger_plugin.run(ctx)

    # Auto-install EAV check triggers on the API view.
    from pgcraft.factory.dimension.eav import (  # noqa: PLC0415
        PGCraftEAV,
    )
    from pgcraft.plugins.check import (  # noqa: PLC0415
        TriggerCheckPlugin,
    )

    if isinstance(source, PGCraftEAV):
        check_plugin = TriggerCheckPlugin(table_key="api")
        check_plugin.run(ctx)


class APIView:
    """Create a PostgREST-facing view with auto-selected triggers.

    Reads from a table factory's context to create an API view
    and INSTEAD OF triggers.  The trigger strategy is
    auto-selected based on the factory type
    (``source.TRIGGER_PLUGIN_CLS``).

    Grants drive triggers: only operations listed in *grants*
    get INSTEAD OF triggers.  ``"select"``-only views have no
    triggers and are read-only.

    Args:
        source: A :class:`~pgcraft.factory.base.ResourceFactory`
            instance whose table/context to expose.
        schema: Schema for the API view (default ``"api"``).
        grants: PostgREST privileges (default ``["select"]``).
            Determines which INSTEAD OF triggers are created.
        query: Optional callable ``(query, source_table) ->
            Select`` for SQLAlchemy-style view customization
            (joins, column filtering, etc.).
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
        columns: list[str] | None = None,
        exclude_columns: list[str] | None = None,
    ) -> None:
        """Create the API view and triggers."""
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
            writable: list[str] | None = [c for c in effective if c in dim_set]
        elif query is not None:
            # query= may add non-table columns (joins, etc.)
            # so restrict triggers to base dim columns.
            writable = list(ctx.dim_column_names)
        else:
            writable = None

        # Derive DML operations from grants.
        dml_ops: list[str] = [
            g for g in self.grants if g in {"insert", "update", "delete"}
        ]

        register_api_resource(
            ctx.metadata,
            APIResource(
                name=ctx.tablename,
                schema=schema,
                grants=self.grants,
            ),
        )

        if dml_ops:
            _install_triggers(source, ctx, writable, dml_ops)
