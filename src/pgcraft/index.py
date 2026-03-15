"""Index support for pgcraft dimensions.

Provides :class:`PGCraftIndex`, a declarative index definition
that uses ``{column_name}`` markers, consistent with
:class:`~pgcraft.check.PGCraftCheck`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from pgcraft.validation import extract_column_names, resolve_markers

if TYPE_CHECKING:
    from collections.abc import Callable


@dataclass(frozen=True)
class PGCraftIndex:
    """A declarative index definition with ``{col}`` markers.

    Each entry in ``expressions`` is a string that may contain
    ``{column_name}`` markers.  Simple column references
    (``"{name}"``) and functional expressions
    (``"lower({name})"``) are both supported.

    Args:
        expressions: Index expressions using ``{column_name}``
            markers, e.g. ``["{name}"]`` or
            ``["lower({code})"]``.
        name: Required index name — no auto-naming.
        unique: Whether to create a unique index.

    """

    expressions: list[str] = field(default_factory=list)
    name: str = ""
    unique: bool = False

    def column_names(self) -> list[str]:
        """Extract ``{name}`` markers from all expressions.

        Returns:
            Column names in order of first appearance,
            deduplicated across all expressions.

        """
        seen: set[str] = set()
        result: list[str] = []
        for expr in self.expressions:
            for name in extract_column_names(expr):
                if name not in seen:
                    seen.add(name)
                    result.append(name)
        return result

    def resolve(self, mapping: Callable[[str], str]) -> list[str]:
        """Replace ``{col}`` markers in each expression.

        Args:
            mapping: A callable that maps column names to their
                resolved form.

        Returns:
            List of resolved expression strings.

        """
        return [resolve_markers(expr, mapping) for expr in self.expressions]


def collect_indices(
    schema_items: list,
) -> list[PGCraftIndex]:
    """Filter :class:`PGCraftIndex` instances from a schema items list.

    Args:
        schema_items: Mixed list of ``Column``,
            ``PGCraftIndex``, and other schema items.

    Returns:
        Only the ``PGCraftIndex`` items, in their original order.

    """
    return [i for i in schema_items if isinstance(i, PGCraftIndex)]
