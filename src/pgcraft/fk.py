"""Foreign key support for pgcraft dimensions.

Provides :class:`PGCraftFK`, a declarative foreign key constraint
definition that pairs ``{column_name}`` markers with their target
references in a single dict.

Two reference modes are supported:

- ``references`` — a dict mapping ``{local_col}`` markers to
  ``"dimension.column"`` strings, resolved at factory time via
  the dimension registry in
  ``metadata.info["pgcraft_dimensions"]``.
- ``raw_references`` — a dict mapping ``{local_col}`` markers to
  ``"schema.table.column"`` strings, passed through to SQLAlchemy
  directly.

Exactly one of the two must be provided.
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
    """Resolve a ``"dimension.column"`` reference.

    Looks up the dimension name in the registry and expands it
    to ``"schema.table.column"``.

    Args:
        metadata: SQLAlchemy MetaData for registry lookup.
        reference: Two-part ``"dimension.column"`` string.

    Returns:
        Fully qualified ``"schema.table.column"`` string.

    Raises:
        PGCraftValidationError: If the reference does not
            contain exactly one dot, or names an unknown
            dimension.

    """
    parts = reference.split(".")
    if len(parts) != 2:  # noqa: PLR2004
        msg = (
            f"FK reference {reference!r} must be "
            f"'dimension.column' format. "
            f"Use raw_references for "
            f"'schema.table.column' references."
        )
        raise PGCraftValidationError(msg)

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
            f"Use raw_references for "
            f"'schema.table.column' references."
        )
        raise PGCraftValidationError(msg)

    ref = registry[dim_name]
    return f"{ref.schema}.{ref.table}.{col_name}"


@dataclass(frozen=True)
class PGCraftFK:
    """A declarative foreign key constraint.

    Each entry in the dict maps a local ``{column}`` marker to
    its target reference.  Exactly one of ``references`` or
    ``raw_references`` must be provided:

    - ``references`` — targets use ``"dimension.column"`` format,
      resolved via the dimension registry::

          PGCraftFK(
              references={"{customer_id}": "customers.id"},
              name="fk_orders_customer",
          )

    - ``raw_references`` — targets use
      ``"schema.table.column"`` format, passed through
      directly::

          PGCraftFK(
              raw_references={"{org_id}": "public.orgs.id"},
              name="fk_orders_org",
          )

    Args:
        references: Mapping of ``{col}`` markers to dimension
            references (resolved via registry).
        raw_references: Mapping of ``{col}`` markers to literal
            ``schema.table.column`` references.
        name: Required constraint name — no auto-naming.
        ondelete: ON DELETE action (e.g. ``"CASCADE"``).
        onupdate: ON UPDATE action (e.g. ``"CASCADE"``).

    Raises:
        PGCraftValidationError: If both or neither of
            ``references`` and ``raw_references`` are provided.

    """

    references: dict[str, str] = field(default_factory=dict)
    raw_references: dict[str, str] = field(default_factory=dict)
    name: str = ""
    ondelete: str | None = None
    onupdate: str | None = None

    def __post_init__(self) -> None:
        """Validate that exactly one reference mode is used."""
        has_refs = bool(self.references)
        has_raw = bool(self.raw_references)
        if has_refs and has_raw:
            msg = (
                f"PGCraftFK {self.name!r}: provide either "
                f"'references' or 'raw_references', not both."
            )
            raise PGCraftValidationError(msg)
        if not has_refs and not has_raw:
            msg = (
                f"PGCraftFK {self.name!r}: provide either "
                f"'references' or 'raw_references'."
            )
            raise PGCraftValidationError(msg)

    @property
    def _mapping(self) -> dict[str, str]:
        """Return whichever reference dict was provided."""
        return self.references or self.raw_references

    def column_names(self) -> list[str]:
        """Extract ``{name}`` markers from local column keys.

        Returns:
            Column names in order of first appearance,
            deduplicated.

        """
        seen: set[str] = set()
        result: list[str] = []
        for col in self._mapping:
            for name in extract_column_names(col):
                if name not in seen:
                    seen.add(name)
                    result.append(name)
        return result

    def resolve(self, mapping: Callable[[str], str]) -> list[str]:
        """Replace ``{col}`` markers in local column keys.

        Args:
            mapping: A callable that maps column names to their
                resolved form.

        Returns:
            List of resolved column name strings.

        """
        return [resolve_markers(col, mapping) for col in self._mapping]

    def resolve_references(
        self,
        metadata: Any,  # noqa: ANN401
    ) -> list[str]:
        """Resolve target references to full FK strings.

        If ``raw_references`` was used, returns values as-is.
        If ``references`` was used, resolves each via the
        dimension registry.

        Args:
            metadata: SQLAlchemy MetaData for registry lookup.

        Returns:
            List of fully qualified FK reference strings.

        """
        if self.raw_references:
            return list(self.raw_references.values())
        return [
            resolve_fk_reference(metadata, ref)
            for ref in self.references.values()
        ]


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
