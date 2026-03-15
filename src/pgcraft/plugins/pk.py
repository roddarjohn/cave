"""Primary key plugins."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import Column, Integer, text
from sqlalchemy.dialects.postgresql import UUID

if TYPE_CHECKING:
    from pgcraft.factory.context import FactoryContext

from pgcraft.columns import PrimaryKeyColumns
from pgcraft.plugin import (
    MinPGVersion,
    Plugin,
    produces,
    requires,
    singleton,
)


@produces("pk_columns")
@singleton("__pk__")
class SerialPKPlugin(Plugin):
    """Provide an auto-increment integer primary key column.

    Args:
        column_name: Name of the PK column (default ``"id"``).

    """

    def __init__(self, column_name: str = "id") -> None:
        """Store the PK column name."""
        self._column_name = column_name

    def run(self, ctx: FactoryContext) -> None:
        """Store a PrimaryKeyColumns in the ctx store."""
        ctx["pk_columns"] = PrimaryKeyColumns(
            [Column(self._column_name, Integer, primary_key=True)]
        )


@produces("pk_columns")
@singleton("__pk__")
class UUIDV4PKPlugin(Plugin):
    """Provide a UUIDv4 primary key column.

    Uses PostgreSQL's ``gen_random_uuid()`` as the server default
    so rows get a unique identifier without client-side generation.

    Args:
        column_name: Name of the PK column (default ``"id"``).

    """

    def __init__(self, column_name: str = "id") -> None:
        """Store the PK column name."""
        self._column_name = column_name

    def run(self, ctx: FactoryContext) -> None:
        """Store a PrimaryKeyColumns in the ctx store."""
        ctx["pk_columns"] = PrimaryKeyColumns(
            [
                Column(
                    self._column_name,
                    UUID(as_uuid=True),
                    primary_key=True,
                    server_default=text("gen_random_uuid()"),
                )
            ]
        )


@requires(MinPGVersion(18))
@produces("pk_columns")
@singleton("__pk__")
class UUIDV7PKPlugin(Plugin):
    """Provide a UUIDv7 primary key column.

    Uses PostgreSQL 18's ``uuidv7()`` as the server default to
    generate time-ordered UUIDs.  These sort chronologically,
    making them friendlier to B-tree indexes than random UUIDv4
    values.

    Requires PostgreSQL 18 or later (declared via
    ``@requires(MinPGVersion(18))``).  Use
    :func:`~pgcraft.plugin.check_pg_version` to validate the
    server version before applying DDL.

    Args:
        column_name: Name of the PK column (default ``"id"``).

    """

    def __init__(self, column_name: str = "id") -> None:
        """Store the PK column name."""
        self._column_name = column_name

    def run(self, ctx: FactoryContext) -> None:
        """Store a PrimaryKeyColumns in the ctx store."""
        ctx["pk_columns"] = PrimaryKeyColumns(
            [
                Column(
                    self._column_name,
                    UUID(as_uuid=True),
                    primary_key=True,
                    server_default=text("uuidv7()"),
                )
            ]
        )
