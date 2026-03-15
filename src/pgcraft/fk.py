"""Foreign key support for pgcraft dimensions.

Provides :class:`PGCraftFK`, a declarative foreign key constraint
definition using SQLAlchemy FK string format for references.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class PGCraftFK:
    """A declarative foreign key constraint definition.

    Args:
        columns: Local column names participating in the FK.
        references: Target columns in SQLAlchemy FK string format
            (e.g. ``["schema.table.column"]``).
        name: Required constraint name — no auto-naming.
        ondelete: ON DELETE action (e.g. ``"CASCADE"``).
        onupdate: ON UPDATE action (e.g. ``"CASCADE"``).

    """

    columns: list[str] = field(default_factory=list)
    references: list[str] = field(default_factory=list)
    name: str = ""
    ondelete: str | None = None
    onupdate: str | None = None


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
