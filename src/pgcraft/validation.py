"""Shared column-reference validation for pgcraft plugins."""

from __future__ import annotations

from pgcraft.errors import PGCraftValidationError


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
