"""Computed column support for pgcraft dimensions.

Provides :class:`PGCraftComputed`, a declarative computed column that
uses ``{column_name}`` markers in its SQL expression.  These are
view-only virtual columns resolved by the API plugin into
``literal_column`` expressions.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy import types as sa_types

_MARKER_RE = re.compile(r"\{(\w+)\}")


@dataclass(frozen=True)
class PGCraftComputed:
    """A declarative computed column with ``{col}`` markers.

    Args:
        name: Output column name in the API view.
        expression: SQL expression using ``{column_name}``
            markers, e.g. ``"{first_name} || ' ' || {last_name}"``.
        type: SQLAlchemy type engine for the proxy column.

    """

    name: str
    expression: str
    type: sa_types.TypeEngine

    def column_names(self) -> list[str]:
        """Extract ``{name}`` markers from the expression.

        Returns:
            List of column names referenced in the expression,
            in order of first appearance with duplicates removed.

        """
        seen: set[str] = set()
        result: list[str] = []
        for m in _MARKER_RE.finditer(self.expression):
            col = m.group(1)
            if col not in seen:
                seen.add(col)
                result.append(col)
        return result

    def resolve(self, mapping: Callable[[str], str]) -> str:
        """Replace each ``{col}`` with ``mapping(col)``.

        Args:
            mapping: A callable that maps column names to their
                resolved form.

        Returns:
            The resolved SQL expression.

        """
        return _MARKER_RE.sub(lambda m: mapping(m.group(1)), self.expression)


def collect_computed(
    schema_items: list,
) -> list[PGCraftComputed]:
    """Filter :class:`PGCraftComputed` from a schema items list.

    Args:
        schema_items: Mixed list of ``Column``,
            ``PGCraftComputed``, and other schema items.

    Returns:
        Only the ``PGCraftComputed`` items, in original order.

    """
    return [i for i in schema_items if isinstance(i, PGCraftComputed)]
