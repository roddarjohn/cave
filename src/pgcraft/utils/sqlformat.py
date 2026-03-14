"""SQL formatting via sqlfluff.

Uses the sqlfluff Python API (``sqlfluff.fix``) to format SQL
strings programmatically.
"""

import sqlfluff


def format_sql(sql: str) -> str:
    """Format a SQL statement using sqlfluff.

    Returns the formatted result.  If sqlfluff fails (e.g. a
    parse error on PL/pgSQL bodies), the original SQL is returned
    unchanged.
    """
    try:
        return sqlfluff.fix(sql, dialect="postgres")
    except Exception:  # noqa: BLE001
        return sql
