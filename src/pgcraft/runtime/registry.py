"""DimensionRegistry — audit and status table for runtime-generated schemas.

The registry tracks every attempt to apply a
:class:`~pgcraft.runtime.config.DimensionConfig` to a tenant schema.  Each
row records the config that was submitted, the schema it targeted, when
processing started and finished, whether it succeeded, and the SQL that was
actually executed (for audit purposes).

The table lives in the ``pgcraft`` schema so it is isolated from tenant
schemas and never touched by the tenant-scoped autogenerate comparator.

Usage::

    from sqlalchemy import create_engine
    from pgcraft.runtime.registry import (
        DimensionRegistry,
        ensure_registry_table,
    )

    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        ensure_registry_table(conn)

        entry = DimensionRegistry.create(conn, config=cfg, schema="tenant_abc")
        # ... process ...
        entry.mark_done(conn, sql="CREATE TABLE ...")

"""

from __future__ import annotations

import enum
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from sqlalchemy import (
    Column,
    DateTime,
    Enum,
    Integer,
    MetaData,
    String,
    Table,
    text,
)
from sqlalchemy import Text as SAText

if TYPE_CHECKING:
    from sqlalchemy import Connection

    from pgcraft.runtime.config import DimensionConfig


# ---------------------------------------------------------------------------
# Schema bootstrap
# ---------------------------------------------------------------------------

_REGISTRY_SCHEMA = "pgcraft"
_REGISTRY_TABLE = "dimension_registry"

_metadata = MetaData()

_registry_table = Table(
    _REGISTRY_TABLE,
    _metadata,
    Column("id", Integer, primary_key=True),
    Column(
        "created_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    ),
    Column("finished_at", DateTime(timezone=True), nullable=True),
    Column("schema_name", String(63), nullable=False),
    Column("table_name", String(63), nullable=False),
    Column("config_json", SAText, nullable=False),
    Column(
        "status",
        Enum(
            "pending",
            "applying",
            "done",
            "error",
            name="dimension_registry_status",
            schema=_REGISTRY_SCHEMA,
        ),
        nullable=False,
        server_default=text("'pending'"),
    ),
    Column("error_message", SAText, nullable=True),
    Column("applied_sql", SAText, nullable=True),
    schema=_REGISTRY_SCHEMA,
)


def ensure_registry_table(conn: Connection) -> None:
    """Create the ``pgcraft.dimension_registry`` table if it does not exist.

    Idempotent — safe to call on every startup.

    Args:
        conn: An active SQLAlchemy connection.  The caller is responsible
            for committing the transaction.

    """
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {_REGISTRY_SCHEMA}"))
    _metadata.create_all(conn, tables=[_registry_table], checkfirst=True)


# ---------------------------------------------------------------------------
# Status enum
# ---------------------------------------------------------------------------


class RegistryStatus(enum.StrEnum):
    """Lifecycle states for a :class:`DimensionRegistry` entry."""

    pending = "pending"
    applying = "applying"
    done = "done"
    error = "error"


# ---------------------------------------------------------------------------
# Registry entry wrapper
# ---------------------------------------------------------------------------


class DimensionRegistry:
    """Thin wrapper around a ``dimension_registry`` row.

    Instances are created via :meth:`create` (which inserts a row) and
    updated via :meth:`mark_applying`, :meth:`mark_done`, and
    :meth:`mark_error`.

    Args:
        row_id: The primary key of the registry row.

    """

    def __init__(self, row_id: int) -> None:
        """Store the registry row id."""
        self._id = row_id

    @property
    def id(self) -> int:
        """Primary key of the registry row."""
        return self._id

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def create(
        cls,
        conn: Connection,
        *,
        config: DimensionConfig,
        schema: str,
    ) -> DimensionRegistry:
        """Insert a new *pending* registry entry and return the wrapper.

        Args:
            conn: An active SQLAlchemy connection.
            config: The dimension configuration being applied.
            schema: The tenant schema being targeted.

        Returns:
            A :class:`DimensionRegistry` bound to the new row.

        """
        result = conn.execute(
            _registry_table.insert()
            .values(
                schema_name=schema,
                table_name=config.table_name,
                config_json=config.model_dump_json(),
                status=RegistryStatus.pending,
            )
            .returning(_registry_table.c.id)
        )
        row_id: int = result.scalar_one()
        return cls(row_id)

    # ------------------------------------------------------------------
    # State transitions
    # ------------------------------------------------------------------

    def mark_applying(self, conn: Connection) -> None:
        """Transition this entry to *applying* status.

        Args:
            conn: An active SQLAlchemy connection.

        """
        conn.execute(
            _registry_table.update()
            .where(_registry_table.c.id == self._id)
            .values(status=RegistryStatus.applying)
        )

    def mark_done(self, conn: Connection, *, sql: str) -> None:
        r"""Transition this entry to *done* and record the executed SQL.

        Args:
            conn: An active SQLAlchemy connection.
            sql: All SQL statements that were applied, joined by ``"\n\n"``.

        """
        conn.execute(
            _registry_table.update()
            .where(_registry_table.c.id == self._id)
            .values(
                status=RegistryStatus.done,
                finished_at=datetime.now(tz=UTC),
                applied_sql=sql,
            )
        )

    def mark_error(self, conn: Connection, *, message: str) -> None:
        """Transition this entry to *error* and record the failure message.

        Args:
            conn: An active SQLAlchemy connection.
            message: Human-readable description of the failure.

        """
        conn.execute(
            _registry_table.update()
            .where(_registry_table.c.id == self._id)
            .values(
                status=RegistryStatus.error,
                finished_at=datetime.now(tz=UTC),
                error_message=message,
            )
        )
