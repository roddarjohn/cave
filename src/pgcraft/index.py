"""Index support for pgcraft dimensions.

Provides :class:`PGCraftIndex`, a declarative index definition
with plain column names (no ``{}`` markers).
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class PGCraftIndex:
    """A declarative index definition.

    Args:
        columns: Column names to include in the index.
        name: Required index name — no auto-naming.
        unique: Whether to create a unique index.

    """

    columns: list[str] = field(default_factory=list)
    name: str = ""
    unique: bool = False


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
