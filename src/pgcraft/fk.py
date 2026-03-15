"""Foreign key support for pgcraft dimensions.

Provides :class:`PGCraftFK`, a declarative foreign key constraint
definition that uses ``{column_name}`` markers for local columns.

References use a two-part ``"dimension.column"`` format that is
resolved at factory time to the correct physical table via the
dimension registry in ``metadata.info["pgcraft_dimensions"]``.
Three-part ``"schema.table.column"`` references bypass resolution
and are passed through to SQLAlchemy directly.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from pgcraft.errors import PGCraftValidationError
from pgcraft.validation import extract_column_names, resolve_markers

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy import MetaData


@dataclass(frozen=True)
class DimensionRef:
    """Registry entry for a dimension's FK-targetable table.

    Stored in ``metadata.info["pgcraft_dimensions"]`` keyed by
    dimension name (``tablename``).

    Args:
        schema: PostgreSQL schema name.
        table: Physical table name for FK targets.

    """

    schema: str
    table: str


def register_dimension(
    metadata: MetaData,
    name: str,
    ref: DimensionRef,
) -> None:
    """Register a dimension for FK resolution.

    Args:
        metadata: SQLAlchemy MetaData instance.
        name: Dimension name (``tablename``).
        ref: The dimension's FK target info.

    """
    registry: dict[str, DimensionRef] = metadata.info.setdefault(
        "pgcraft_dimensions", {}
    )
    registry[name] = ref


def resolve_fk_reference(
    metadata: MetaData,
    reference: str,
) -> str:
    """Resolve a FK reference to a full SQLAlchemy FK string.

    Two-part references (``"dimension.column"``) are resolved via
    the dimension registry.  Three-or-more-part references
    (``"schema.table.column"``) are returned as-is.

    Args:
        metadata: SQLAlchemy MetaData for registry lookup.
        reference: FK reference string.

    Returns:
        Fully qualified ``"schema.table.column"`` string.

    Raises:
        PGCraftValidationError: If a two-part reference names
            an unknown dimension.

    """
    parts = reference.split(".")
    if len(parts) != 2:  # noqa: PLR2004
        return reference

    dim_name, col_name = parts
    registry: dict[str, DimensionRef] = metadata.info.get(
        "pgcraft_dimensions", {}
    )
    if dim_name not in registry:
        known = sorted(registry)
        msg = (
            f"FK reference {reference!r} names unknown "
            f"dimension {dim_name!r}. "
            f"Known dimensions: {known}. "
            f"Use 'schema.table.column' format to bypass "
            f"dimension resolution."
        )
        raise PGCraftValidationError(msg)

    ref = registry[dim_name]
    return f"{ref.schema}.{ref.table}.{col_name}"


@dataclass(frozen=True)
class PGCraftFK:
    """A declarative foreign key constraint with ``{col}`` markers.

    Local columns use ``{column_name}`` markers that get resolved
    against the target table.

    References use a two-part ``"dimension.column"`` format that
    is resolved via the dimension registry, or a three-part
    ``"schema.table.column"`` format passed through directly.

    Args:
        columns: Local column markers, e.g. ``["{org_id}"]``.
        references: Target references, e.g.
            ``["customer.id"]`` (resolved) or
            ``["dim.customer.id"]`` (passthrough).
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

    def resolve_references(
        self,
        metadata: Any,  # noqa: ANN401
    ) -> list[str]:
        """Resolve all references via the dimension registry.

        Args:
            metadata: SQLAlchemy MetaData for registry lookup.

        Returns:
            List of fully qualified FK reference strings.

        """
        return [resolve_fk_reference(metadata, ref) for ref in self.references]


def collect_fks(
    schema_items: list,
) -> list[PGCraftFK]:
    """Filter :class:`PGCraftFK` instances from schema items.

    Args:
        schema_items: Mixed list of schema items.

    Returns:
        Only the ``PGCraftFK`` items, in original order.

    """
    return [i for i in schema_items if isinstance(i, PGCraftFK)]
