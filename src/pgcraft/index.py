"""Index support for pgcraft dimensions.

Provides :class:`PGCraftIndex`, a declarative index definition
that mirrors ``sqlalchemy.Index`` and uses ``{column_name}``
markers for column references.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pgcraft.validation import extract_column_names, resolve_markers

if TYPE_CHECKING:
    from collections.abc import Callable


class PGCraftIndex:
    """A declarative index definition with ``{col}`` markers.

    Mirrors the ``sqlalchemy.Index`` constructor signature::

        PGCraftIndex("idx_name", "{col1}", "{col2}",
                     unique=True, postgresql_using="btree")

    Simple column references (``"{name}"``) and functional
    expressions (``"lower({name})"``) are both supported.
    Extra keyword arguments are passed through to the
    underlying ``sqlalchemy.Index``.

    Args:
        name: Required index name.
        *expressions: Index expressions using ``{column_name}``
            markers.
        unique: Whether to create a unique index.
        **kw: Passed through to ``sqlalchemy.Index``
            (e.g. ``postgresql_using``, ``postgresql_where``).

    """

    __slots__ = ("expressions", "kw", "name", "unique")

    name: str
    expressions: list[str]
    unique: bool
    kw: dict[str, Any]

    def __init__(
        self,
        name: str,
        *expressions: str,
        unique: bool = False,
        **kw: Any,  # noqa: ANN401
    ) -> None:
        """Create a new index definition."""
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "expressions", list(expressions))
        object.__setattr__(self, "unique", unique)
        object.__setattr__(self, "kw", dict(kw))

    def __setattr__(
        self,
        key: str,
        value: object,
    ) -> None:
        """Prevent mutation after construction."""
        msg = "PGCraftIndex instances are immutable"
        raise AttributeError(msg)

    def __repr__(self) -> str:
        """Return a constructor-style repr."""
        parts = [repr(self.name)]
        parts.extend(repr(e) for e in self.expressions)
        if self.unique:
            parts.append("unique=True")
        for k, v in self.kw.items():
            parts.append(f"{k}={v!r}")
        return f"PGCraftIndex({', '.join(parts)})"

    def __eq__(self, other: object) -> bool:
        """Compare by name, expressions, unique, and kw."""
        if not isinstance(other, PGCraftIndex):
            return NotImplemented
        return (
            self.name == other.name
            and self.expressions == other.expressions
            and self.unique == other.unique
            and self.kw == other.kw
        )

    def __hash__(self) -> int:
        """Hash by name, expressions, and unique flag."""
        return hash(
            (
                self.name,
                tuple(self.expressions),
                self.unique,
            )
        )

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
            mapping: A callable that maps column names to
                their resolved form.

        Returns:
            List of resolved expression strings.

        """
        return [resolve_markers(expr, mapping) for expr in self.expressions]


def collect_indices(
    schema_items: list,
) -> list[PGCraftIndex]:
    """Filter :class:`PGCraftIndex` instances from schema items.

    Args:
        schema_items: Mixed list of schema items.

    Returns:
        Only the ``PGCraftIndex`` items, in original order.

    """
    return [i for i in schema_items if isinstance(i, PGCraftIndex)]
