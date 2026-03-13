"""Statistics view support for pgcraft dimensions.

Provides :class:`PGCraftStatistics`, a declarative statistics view
definition that can be joined into the API view as read-only fields.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy import types as sa_types


@dataclass(frozen=True)
class PGCraftStatistics:
    """A declarative statistics view definition.

    Args:
        name: View suffix (e.g. ``"statistics"`` produces
            ``"{tablename}_statistics"``).
        query: Raw SQL for the view body.
        columns: Output column specs as ``(name, type)`` pairs,
            excluding the join key column.
        materialized: Whether to create a materialized view.
        join_key: Column name used to join back to the primary
            table. Defaults to the PK column name at runtime.

    """

    name: str
    query: str
    columns: list[tuple[str, sa_types.TypeEngine]] = field(default_factory=list)
    materialized: bool = False
    join_key: str | None = None

    @property
    def column_names(self) -> list[str]:
        """Return output column names (excluding join key).

        Returns:
            List of column names from the columns spec.

        """
        return [name for name, _ in self.columns]


@dataclass(frozen=True)
class StatisticsViewInfo:
    """Runtime info about a created statistics view.

    Stored in ``ctx[stats_key]`` by the statistics plugin for
    the API plugin to consume when building LEFT JOINs.

    Args:
        view_name: Fully qualified view name
            (e.g. ``"dim.customer_statistics"``).
        join_key: Column name used for the JOIN condition.
        column_names: Output column names to include in SELECT.

    """

    view_name: str
    join_key: str
    column_names: list[str]


def collect_statistics(
    schema_items: list,
) -> list[PGCraftStatistics]:
    """Filter :class:`PGCraftStatistics` from a schema items list.

    Args:
        schema_items: Mixed list of ``Column``,
            ``PGCraftStatistics``, and other schema items.

    Returns:
        Only the ``PGCraftStatistics`` items, in original order.

    """
    return [i for i in schema_items if isinstance(i, PGCraftStatistics)]
