"""SQL formatting via pglast.

Uses ``pglast.prettify`` to format SQL strings programmatically.
pglast wraps the real PostgreSQL parser (libpg_query) and is
orders of magnitude faster than sqlfluff for per-statement
formatting.

Why pglast here and sqlfluff elsewhere: sqlfluff is used for
linting ``.sql`` files on disk (``just sql-lint``) because it
enforces style rules (capitalisation, aliasing, etc.) that pglast
does not cover.  But sqlfluff is far too slow (~60 ms/statement)
to run 300+ times during ``alembic revision --autogenerate``.
pglast handles the same pretty-printing job at ~0.3 ms/statement.
"""

import pglast

# Default compact-lists margin: keeps comma-separated items on one
# line as long as the result fits within this many characters.
_DEFAULT_MARGIN = 76


def format_sql(
    sql: str,
    *,
    compact_lists_margin: int = _DEFAULT_MARGIN,
) -> str:
    """Format a SQL statement using pglast.

    Args:
        sql: Raw SQL string.
        compact_lists_margin: Column budget for inline lists
            (default 76, allowing 4-char indent to stay
            within 80).

    Returns:
        The formatted SQL, or the original if pglast
        cannot parse it (e.g. PL/pgSQL bodies).

    """
    try:
        return pglast.prettify(
            sql,
            compact_lists_margin=compact_lists_margin,
        )
    except Exception:  # noqa: BLE001
        return sql
