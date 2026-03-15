"""Check constraint support for pgcraft dimensions.

Provides :class:`PGCraftCheck`, a declarative check constraint that uses
``{column_name}`` markers in its expression.  Plugins resolve these
markers to the appropriate column references depending on the
dimension type (table-level for simple/append-only, ``NEW.col`` for
EAV trigger-based enforcement).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from pgcraft.validation import extract_column_names, resolve_markers

if TYPE_CHECKING:
    from collections.abc import Callable


@dataclass(frozen=True)
class PGCraftCheck:
    """A declarative check constraint with ``{col}`` markers.

    Args:
        expression: Constraint expression using ``{column_name}``
            markers, e.g. ``"{price} > 0"``.
        name: Required constraint name — no auto-naming.

    """

    expression: str
    name: str

    def column_names(self) -> list[str]:
        """Extract ``{name}`` markers from the expression.

        Returns:
            List of column names referenced in the expression,
            in order of first appearance with duplicates removed.

        """
        return extract_column_names(self.expression)

    def resolve(self, mapping: Callable[[str], str]) -> str:
        """Replace each ``{col}`` with ``mapping(col)``.

        Args:
            mapping: A callable that maps column names to their
                resolved form (e.g. identity for table-level,
                ``lambda c: f"NEW.{c}"`` for triggers).

        Returns:
            The resolved SQL expression.

        """
        return resolve_markers(self.expression, mapping)


def collect_checks(schema_items: list) -> list[PGCraftCheck]:
    """Filter :class:`PGCraftCheck` instances from a schema items list.

    Args:
        schema_items: Mixed list of ``Column``, ``PGCraftCheck``, and
            other schema items.

    Returns:
        Only the ``PGCraftCheck`` items, in their original order.

    """
    return [i for i in schema_items if isinstance(i, PGCraftCheck)]
