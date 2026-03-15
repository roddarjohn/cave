"""Foreign key support for pgcraft dimensions.

Provides :class:`PGCraftFK`, a declarative foreign key constraint
definition that uses ``{column_name}`` markers for local columns,
consistent with :class:`~pgcraft.check.PGCraftCheck`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from pgcraft.validation import extract_column_names, resolve_markers

if TYPE_CHECKING:
    from collections.abc import Callable


@dataclass(frozen=True)
class PGCraftFK:
    """A declarative foreign key constraint with ``{col}`` markers.

    Local columns use ``{column_name}`` markers that get resolved
    against the target table.  References use SQLAlchemy FK string
    format (``"schema.table.column"``).

    Args:
        columns: Local column markers, e.g. ``["{org_id}"]``.
        references: Target columns in SQLAlchemy FK string format
            (e.g. ``["dim.org.id"]``).
        name: Required constraint name — no auto-naming.
        ondelete: ON DELETE action (e.g. ``"CASCADE"``).
        onupdate: ON UPDATE action (e.g. ``"CASCADE"``).

    """

    columns: list[str] = field(default_factory=list)
    references: list[str] = field(default_factory=list)
    name: str = ""
    ondelete: str | None = None
    onupdate: str | None = None

    def column_names(self) -> list[str]:
        """Extract ``{name}`` markers from local columns.

        Returns:
            Column names in order of first appearance,
            deduplicated across all column entries.

        """
        seen: set[str] = set()
        result: list[str] = []
        for col in self.columns:
            for name in extract_column_names(col):
                if name not in seen:
                    seen.add(name)
                    result.append(name)
        return result

    def resolve(self, mapping: Callable[[str], str]) -> list[str]:
        """Replace ``{col}`` markers in local columns.

        Args:
            mapping: A callable that maps column names to their
                resolved form.

        Returns:
            List of resolved column name strings.

        """
        return [resolve_markers(col, mapping) for col in self.columns]


def collect_fks(
    schema_items: list,
) -> list[PGCraftFK]:
    """Filter :class:`PGCraftFK` instances from a schema items list.

    Args:
        schema_items: Mixed list of ``Column``, ``PGCraftFK``,
            and other schema items.

    Returns:
        Only the ``PGCraftFK`` items, in their original order.

    """
    return [i for i in schema_items if isinstance(i, PGCraftFK)]
