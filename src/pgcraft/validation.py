"""Shared column-reference validation and marker helpers.

All pgcraft constraint types (:class:`~pgcraft.check.PGCraftCheck`,
:class:`~pgcraft.index.PGCraftIndex`, :class:`~pgcraft.fk.PGCraftFK`)
use ``{column_name}`` markers.  This module provides the regex, extraction,
resolution, and validation helpers they share.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from pgcraft.errors import PGCraftValidationError

if TYPE_CHECKING:
    from collections.abc import Callable

COLUMN_MARKER_RE = re.compile(r"\{(\w+)\}")
"""Regex matching ``{column_name}`` markers in expressions."""


def extract_column_names(expression: str) -> list[str]:
    """Extract ``{name}`` markers from an expression.

    Args:
        expression: String containing ``{column_name}`` markers.

    Returns:
        Column names in order of first appearance, deduplicated.

    """
    seen: set[str] = set()
    result: list[str] = []
    for m in COLUMN_MARKER_RE.finditer(expression):
        name = m.group(1)
        if name not in seen:
            seen.add(name)
            result.append(name)
    return result


def resolve_markers(
    expression: str,
    mapping: Callable[[str], str],
) -> str:
    """Replace each ``{col}`` marker with ``mapping(col)``.

    Args:
        expression: String containing ``{column_name}`` markers.
        mapping: Callable that maps column names to their resolved
            form (e.g. identity for table-level, or
            ``lambda c: f"NEW.{c}"`` for triggers).

    Returns:
        The expression with all markers replaced.

    """
    return COLUMN_MARKER_RE.sub(lambda m: mapping(m.group(1)), expression)


def validate_column_references(
    label: str,
    columns: list[str],
    known_columns: set[str],
) -> None:
    """Raise if any column is not in *known_columns*.

    Shared by check, index, and foreign-key plugins to ensure
    that user-provided column names actually exist on the target
    table or view.

    Args:
        label: Human-readable name for error messages
            (e.g. ``"PGCraftCheck 'pos_price'"``).
        columns: Column names to validate.
        known_columns: Set of known column names.

    Raises:
        PGCraftValidationError: If a column is not in
            *known_columns*.

    """
    for col in columns:
        if col not in known_columns:
            msg = (
                f"{label} references unknown column "
                f"{col!r}. "
                f"Known columns: {sorted(known_columns)}"
            )
            raise PGCraftValidationError(msg)
