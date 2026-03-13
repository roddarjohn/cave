"""API view plugin: creates the PostgREST-facing view and resource."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import literal_column, select
from sqlalchemy.dialects import postgresql
from sqlalchemy_declarative_extensions import View, register_view

if TYPE_CHECKING:
    from sqlalchemy import Table

    from pgcraft.factory.context import FactoryContext
    from pgcraft.statistics import StatisticsViewInfo

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


def _build_stats_columns(
    stats: dict[str, StatisticsViewInfo],
    aliases: dict[str, str],
) -> list[Any]:
    """Build select-list entries for statistics view columns.

    Args:
        stats: Statistics view info dict from ctx.
        aliases: Map of stats name to table alias.

    Returns:
        List of labeled column expressions for stats columns.

    """
    result: list[Any] = []
    for name, info in stats.items():
        alias = aliases[name]
        result.extend(
            literal_column(f"{alias}.{col_name}").label(col_name)
            for col_name in info.column_names
        )
    return result


@produces(Dynamic("view_key"))
@requires(Dynamic("table_key"))
class APIPlugin(Plugin):
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
        columns: Column names to include in the view. When
            ``None``, all table columns are included.
        stats_key: Key in ``ctx`` holding statistics view info.
            LEFT JOINs stats views into the API view when
            the key exists and is non-empty.

    """

    def __init__(  # noqa: PLR0913
        self,
        schema: str = "api",
        grants: list[Grant] | None = None,
        table_key: str = "primary",
        view_key: str = "api",
        columns: list[str] | None = None,
        stats_key: str = "statistics_views",
    ) -> None:
        """Store the API configuration and context keys."""
        self.schema = schema
        self.grants: list[Grant] = grants if grants is not None else ["select"]
        self.table_key = table_key
        self.view_key = view_key
        self.columns = columns
        self.stats_key = stats_key

    def run(self, ctx: FactoryContext) -> None:
        """Create the API view and register the resource."""
        primary = ctx[self.table_key]
        if self.stats_key in ctx:
            stats: dict[str, StatisticsViewInfo] = ctx[self.stats_key]
        else:
            stats = {}
        use_alias = self.columns is not None or bool(stats)
        alias = "p"

        if self.columns is not None:
            select_cols = _resolve_columns(primary, self.columns, alias)
        elif use_alias:
            select_cols = [
                literal_column(f"{alias}.{col.key}").label(col.key)
                for col in primary.columns
            ]
        else:
            select_cols = [col.label(col.key) for col in primary.columns]

        if stats:
            aliases: dict[str, str] = {}
            for i, name in enumerate(stats):
                aliases[name] = f"s{i}" if i > 0 else "s"
            select_cols.extend(_build_stats_columns(stats, aliases))

        if stats:
            definition = self._build_stats_sql(
                ctx,
                primary,
                select_cols,
                stats,
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
    def _build_stats_sql(  # noqa: PLR0913
        ctx: FactoryContext,
        primary: Table,
        select_cols: list[Any],
        stats: dict[str, StatisticsViewInfo],
        aliases: dict[str, str],
        alias: str,
    ) -> str:
        """Build raw SQL with LEFT JOINs for statistics views."""
        col_strs = []
        for col_expr in select_cols:
            compiled = col_expr.compile(dialect=postgresql.dialect())
            col_strs.append(str(compiled))

        table_ref = f"{ctx.schemaname}.{primary.name}"
        from_clause = f"{table_ref} AS {alias}"

        join_clauses = []
        pk_col_name = ctx["pk_columns"].first_key
        for name, info in stats.items():
            sa = aliases[name]
            join_clauses.append(
                f"LEFT OUTER JOIN {info.view_name}"
                f" AS {sa}"
                f" ON {alias}.{pk_col_name}"
                f" = {sa}.{info.join_key}"
            )

        parts = [
            "SELECT " + ", ".join(col_strs),
            f"\nFROM {from_clause}",
        ]
        parts.extend(f"\n{jc}" for jc in join_clauses)

        return "".join(parts)
