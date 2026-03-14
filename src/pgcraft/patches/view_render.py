"""Patch ``View.render_definition`` to tolerate missing columns.

During autogenerate, ``sqlalchemy-declarative-extensions`` normalizes
view definitions by creating a temporary view in a nested transaction
and reading it back via ``pg_get_viewdef()``.  When the view references
a column that is being added in the same migration, this ``CREATE VIEW``
fails with an ``UndefinedColumn`` error.

This patch catches that error and falls back to pglast-based
normalization, which doesn't require the column to exist yet.
"""

import logging
import uuid

from sqlalchemy import Connection, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy_declarative_extensions.dialects import get_view
from sqlalchemy_declarative_extensions.view.base import (
    View,
    escape_params,
)

from pgcraft.utils.sqlformat import format_sql

logger = logging.getLogger(__name__)

_original_render_definition = View.render_definition


def _patched_render_definition(
    self: View,
    conn: Connection,
    using_connection: bool = True,  # noqa: FBT001, FBT002
) -> str:
    """Render with DB normalization, falling back to pglast on error."""
    dialect = conn.engine.dialect
    compiled_definition = self.compile_definition(dialect)

    if using_connection and dialect.name == "postgresql":
        with conn.begin_nested() as trans:
            try:
                random_name = "v" + uuid.uuid4().hex
                conn.execute(
                    text(f"CREATE VIEW {random_name} AS {compiled_definition}")
                )
                view = get_view(conn, random_name)
                definition1 = view.definition

                if definition1 == compiled_definition:
                    return escape_params(compiled_definition)

                random_name = "v" + uuid.uuid4().hex
                conn.execute(
                    text(f"CREATE VIEW {random_name} AS {definition1}")
                )
                view = get_view(conn, random_name)
                definition2 = view.definition
                return escape_params(definition2)

            except SQLAlchemyError:
                logger.debug(
                    "DB normalization failed for view %r;"
                    " falling back to pglast",
                    self.name,
                    exc_info=True,
                )

            finally:
                trans.rollback()

    # sqlfluff fallback — works without the column existing.
    return escape_params(format_sql(compiled_definition).rstrip("\n")) + ";"


def apply() -> None:
    """Monkey-patch ``View.render_definition``."""
    View.render_definition = _patched_render_definition  # type: ignore[assignment]
