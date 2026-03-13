"""Primary key plugins."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from sqlalchemy import Column, Integer, text
from sqlalchemy.dialects.postgresql import UUID

if TYPE_CHECKING:
    from pgcraft.factory.context import FactoryContext

from pgcraft.columns import PrimaryKeyColumns
from pgcraft.plugin import Plugin, produces, singleton


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


@produces("pk_columns")
@singleton("__pk__")
class UUIDV7PKPlugin(Plugin):
    """Provide a UUIDv7 primary key column stored as a native PostgreSQL UUID.

    Uses ``uuid_generate_v7()`` from the `pg_uuidv7
    <https://github.com/fboulnois/pg_uuidv7>`_ extension as the server
    default. UUIDv7 embeds a millisecond-precision Unix timestamp in the
    most-significant bits, so values sort chronologically and cluster
    well on B-tree indexes — a common pain-point with random UUIDv4 keys.

    The ``pg_uuidv7`` extension is declared in ``required_pg_extensions``
    so the pgcraft Alembic integration automatically emits
    ``CREATE EXTENSION IF NOT EXISTS pg_uuidv7`` in the first migration
    that needs it, without any manual configuration.

    Args:
        column_name: Name of the PK column (default ``"id"``).

    """

    required_pg_extensions: ClassVar[frozenset[str]] = frozenset({"pg_uuidv7"})

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
                    server_default=text("uuid_generate_v7()"),
                )
            ]
        )
