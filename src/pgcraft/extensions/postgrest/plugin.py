"""PostgREST view plugin: creates the PostgREST-facing view and resource."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import literal_column, select
from sqlalchemy.dialects import postgresql
from sqlalchemy_declarative_extensions import View, register_view

if TYPE_CHECKING:
    from sqlalchemy import Table

    from pgcraft.factory.context import FactoryContext
    from pgcraft.statistics import JoinedView

from pgcraft.plugin import Dynamic, Plugin, produces, requires
from pgcraft.resource import (
    APIResource,
    Grant,
    register_api_resource,
)
from pgcraft.utils.query import compile_query


def _resolve_columns(
    primary: Table,
    columns: list[str],
    alias: str = "p",
) -> list[Any]:
    """Build a select list from column names.

    Resolves each name against the table's columns.
    Raises ``ValueError`` for unknown names.

    Args:
        primary: The source table.
        columns: Column names to include.
        alias: Table alias prefix for column references.

    Returns:
        List of labeled column expressions.

    Raises:
        ValueError: If a column name is not found on the table.

    """
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
    """Resolve the effective column list.

    Args:
        primary: The source table.
        columns: Explicit include list (or ``None``).
        exclude_columns: Explicit exclude list (or ``None``).

    Returns:
        Resolved column name list, or ``None`` for all columns.

    Raises:
        ValueError: If both are set, or an excluded column is
            not found on the table.

    """
    if columns is not None and exclude_columns is not None:
        msg = (
            "Cannot specify both 'columns' and"
            " 'exclude_columns' on PostgRESTPlugin"
        )
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


def _build_join_columns(
    joins: dict[str, JoinedView],
    aliases: dict[str, str],
) -> list[Any]:
    """Build select-list entries for joined view columns.

    Args:
        joins: Joined view info dict from ctx.
        aliases: Map of join name to table alias.

    Returns:
        List of labeled column expressions.

    """
    result: list[Any] = []
    for name, view_info in joins.items():
        alias = aliases[name]
        result.extend(
            literal_column(f"{alias}.{col_name}").label(col_name)
            for col_name in view_info.column_names
        )
    return result


@produces(Dynamic("view_key"))
@requires(Dynamic("table_key"))
class PostgRESTPlugin(Plugin):
    """Create a PostgREST-facing view and register its grants.

    Reads ``ctx[table_key]`` to build a view query, registers
    the resulting view, stores it as ``ctx[view_key]``, and
    registers the API resource for role/grant generation.

    Args:
        schema: Schema for the API view (default ``"api"``).
        grants: PostgREST privileges (default ``["select"]``).
        table_key: Key in ``ctx`` to read the source table or
            view proxy from (default ``"primary"``).
        view_key: Key in ``ctx`` to store the created view
            under (default ``"api"``).
        columns: Column names to include in the view. Mutually
            exclusive with ``exclude_columns``.
        exclude_columns: Column names to exclude from the view.
            Mutually exclusive with ``columns``.
        joins_key: Key in ``ctx`` holding a dict of
            :class:`~pgcraft.statistics.JoinedView` entries.
            LEFT JOINs each view into the API view when the key
            exists and is non-empty.

    """

    def __init__(  # noqa: PLR0913
        self,
        schema: str = "api",
        grants: list[Grant] | None = None,
        table_key: str = "primary",
        view_key: str = "api",
        columns: list[str] | None = None,
        exclude_columns: list[str] | None = None,
        joins_key: str = "joins",
    ) -> None:
        """Store the API configuration and context keys."""
        self.schema = schema
        self.grants: list[Grant] = grants if grants is not None else ["select"]
        self.table_key = table_key
        self.view_key = view_key
        self.columns = columns
        self.exclude_columns = exclude_columns
        self.joins_key = joins_key

    def run(self, ctx: FactoryContext) -> None:
        """Create the API view and register the resource."""
        primary = ctx[self.table_key]

        if self.joins_key in ctx:
            joins: dict[str, JoinedView] = ctx[self.joins_key]
        else:
            joins = {}

        effective_columns = _resolve_included_columns(
            primary,
            self.columns,
            self.exclude_columns,
        )

        use_alias = effective_columns is not None or bool(joins)
        alias = "p"

        if effective_columns is not None:
            select_cols = _resolve_columns(
                primary,
                effective_columns,
                alias,
            )
        elif use_alias:
            select_cols = [
                literal_column(f"{alias}.{col.key}").label(col.key)
                for col in primary.columns
            ]
        else:
            select_cols = [col.label(col.key) for col in primary.columns]

        if joins:
            aliases: dict[str, str] = {}
            for i, name in enumerate(joins):
                aliases[name] = f"s{i}" if i > 0 else "s"
            select_cols.extend(_build_join_columns(joins, aliases))

        if joins:
            definition = self._build_join_sql(
                ctx,
                primary,
                select_cols,
                joins,
                aliases,
                alias,
            )
        else:
            source = primary
            if use_alias:
                source = primary.alias(alias)
            query = select(*select_cols).select_from(source)
            definition = compile_query(query)

        view = View(
            ctx.tablename,
            definition,
            schema=self.schema,
        )
        register_view(ctx.metadata, view)
        ctx[self.view_key] = view

        register_api_resource(
            ctx.metadata,
            APIResource(
                name=ctx.tablename,
                schema=self.schema,
                grants=self.grants,
            ),
        )

    @staticmethod
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
