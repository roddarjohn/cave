"""SQL formatting via the sqlfluff CLI.

Wraps ``sqlfluff fix`` in a subprocess to format SQL strings
programmatically.  The CLI is used rather than the Python API
per upstream guidance that the Python API is experimental.
"""

import subprocess
import tempfile
from pathlib import Path


def format_sql(sql: str) -> str:
    """Format a SQL statement by running ``sqlfluff fix``.

    Writes *sql* to a temporary file, runs ``sqlfluff fix`` on it,
    and returns the formatted result.  If sqlfluff fails (e.g. a
    parse error on PL/pgSQL bodies), the original SQL is returned
    unchanged.
    """
    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".sql",
        delete=False,
    ) as tmp:
        tmp.write(sql)
        tmp.flush()
        tmp_path = Path(tmp.name)

    try:
        subprocess.run(  # noqa: S603
            [  # noqa: S607
                "sqlfluff",
                "fix",
                "--dialect",
                "postgres",
                str(tmp_path),
            ],
            capture_output=True,
            check=False,
        )
        return tmp_path.read_text()
    except FileNotFoundError:
        # sqlfluff not installed — return original.
        return sql
    finally:
        tmp_path.unlink(missing_ok=True)
