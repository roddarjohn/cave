"""Statistics view support for pgcraft dimensions.

Provides :class:`PGCraftStatisticsView`, a declarative statistics
view definition that can be joined into the API view as read-only
fields.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy import Select


@dataclass(frozen=True)
class PGCraftStatisticsView:
    """A declarative statistics view definition.

    The ``query`` is a SQLAlchemy :class:`~sqlalchemy.Select` whose
    selected columns define both the view body and the column names
    exposed through the API.  The join key column (used to join
    back to the primary table) should be included in the query but
    is excluded from the API select list automatically.

    The view is named ``{tablename}_{name}_statistics`` — pass
    just the source name (e.g. ``"orders"``), not the full suffix.

    Args:
        name: Source name for the statistics view.  The view
            will be named ``{tablename}_{name}_statistics``
            (e.g. ``"orders"`` → ``"customer_orders_statistics"``).
        query: A SQLAlchemy ``select()`` expression.
        materialized: Whether to create a materialized view.
        join_key: Column name used to join back to the primary
            table.  Defaults to the PK column name at runtime.
        schema: PostgreSQL schema for the view.  Defaults to the
            dimension's schema at runtime.

    Example::

        from sqlalchemy import func, select

        orders = Table("orders", metadata, ...)

        PGCraftStatisticsView(
            name="orders",
            query=select(
                orders.c.customer_id,
                func.count().label("order_count"),
            ).group_by(orders.c.customer_id),
            join_key="customer_id",
        )

    """

    name: str
    query: Select
    materialized: bool = False
    join_key: str | None = None
    schema: str | None = None

    @property
    def view_suffix(self) -> str:
        """Return the view name suffix.

        Returns:
            ``"{name}_statistics"`` — appended to the tablename
            by the statistics plugin.

        """
        return f"{self.name}_statistics"

    @property
    def column_names(self) -> list[str]:
        """Return all column names from the query.

        Returns:
            List of column names derived from the query's
            selected columns.

        """
        return [
            col.key
            for col in self.query.selected_columns
            if col.key is not None
        ]


@dataclass(frozen=True)
class JoinedView:
    """A view to LEFT JOIN into the API view.

    Produced by plugins (e.g. ``StatisticsViewPlugin``) and
    consumed by ``APIPlugin`` when building the API view SQL.

    Args:
        view_name: Fully qualified view name
            (e.g. ``"dim.customer_orders_statistics"``).
        join_key: Column name used for the JOIN condition.
        column_names: Output column names to include in SELECT
            (excludes the join key).

    """

    view_name: str
    join_key: str
    column_names: list[str]


def collect_statistics(
    schema_items: list,
) -> list[PGCraftStatisticsView]:
    """Filter :class:`PGCraftStatisticsView` from a schema items list.

    Args:
        schema_items: Mixed list of ``Column``,
            ``PGCraftStatisticsView``, and other schema items.

    Returns:
        Only the ``PGCraftStatisticsView`` items, in original order.

    """
    return [i for i in schema_items if isinstance(i, PGCraftStatisticsView)]
