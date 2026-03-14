"""Build a SQLAlchemy MetaData from a DimensionConfig.

This is the bridge between the stored config format and the pgcraft factory
system.  It translates the narrow, user-facing config vocabulary (type names,
PK strategy, table type) into the plugin calls that the factory understands,
then registers schemas and roles on the resulting MetaData so the Alembic
autogenerate comparator picks up the full set of schema objects.

Only ``table_type = "simple"`` is supported currently.  Append-only and EAV
types can be added here without touching the config schema.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Integer,
    MetaData,
    Numeric,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID

from pgcraft.alembic.register import pgcraft_configure_metadata
from pgcraft.factory.dimension.simple import SimpleDimensionResourceFactory
from pgcraft.plugins.api import APIPlugin
from pgcraft.plugins.pk import SerialPKPlugin, UUIDV4PKPlugin, UUIDV7PKPlugin
from pgcraft.plugins.simple import SimpleTablePlugin, SimpleTriggerPlugin
from pgcraft.plugins.statistics import StatisticsViewPlugin

if TYPE_CHECKING:
    from sqlalchemy.sql.sqltypes import TypeEngine

    from pgcraft.runtime.config import ColumnConfig, DimensionConfig

# Maps config type names to SQLAlchemy column type instances.
# Using callables for types that require construction (DateTime, UUID).
_COLUMN_TYPE_MAP: dict[str, TypeEngine[object]] = {  # ty: ignore[invalid-assignment]
    "text": Text(),
    "integer": Integer(),
    "bigint": BigInteger(),
    "boolean": Boolean(),
    "timestamptz": DateTime(timezone=True),
    "date": Date(),
    "numeric": Numeric(),
    "uuid": UUID(as_uuid=True),
    "jsonb": JSONB(),
}

# Maps PK config name to the plugin class.
_PK_PLUGIN_MAP = {
    "serial": SerialPKPlugin,
    "uuidv4": UUIDV4PKPlugin,
    "uuidv7": UUIDV7PKPlugin,
}


def _build_column(col: ColumnConfig) -> Column:
    """Translate a :class:`~pgcraft.runtime.config.ColumnConfig` to a Column.

    Args:
        col: The column config to translate.

    Returns:
        A SQLAlchemy :class:`~sqlalchemy.Column` ready to pass to a factory.

    """
    sa_type = _COLUMN_TYPE_MAP[col.type]
    kwargs: dict[str, object] = {"nullable": col.nullable}
    if col.default is not None:
        kwargs["server_default"] = text(col.default)
    return Column(col.name, sa_type, **kwargs)  # ty: ignore[invalid-argument-type]


def build_metadata(config: DimensionConfig, schema: str) -> MetaData:
    """Build a :class:`~sqlalchemy.MetaData` from *config* for *schema*.

    Runs the appropriate pgcraft factory with the configured plugins, which
    registers the backing table, API view, triggers, and any required
    extensions on the returned MetaData.  Also calls
    :func:`~pgcraft.alembic.register.pgcraft_configure_metadata` to register
    schemas and roles so the Alembic autogenerate comparator sees the full
    desired state.

    The caller is responsible for having called
    :func:`~pgcraft.alembic.register.pgcraft_alembic_hook` once before
    using the result with :func:`~pgcraft.runtime.generate.generate_ops`.

    Args:
        config: The validated dimension configuration.
        schema: The PostgreSQL schema to place the table in.

    Returns:
        A populated :class:`~sqlalchemy.MetaData` instance.

    """
    metadata = MetaData()
    columns = [_build_column(col) for col in config.columns]
    pk_plugin = _PK_PLUGIN_MAP[config.pk]()

    # Replace the default PK plugin with the configured one; keep all other
    # default plugins (table, statistics view, API view, triggers).
    SimpleDimensionResourceFactory(
        tablename=config.table_name,
        schemaname=schema,
        metadata=metadata,
        schema_items=columns,  # ty: ignore[invalid-argument-type]
        plugins=[
            pk_plugin,
            SimpleTablePlugin(),
            StatisticsViewPlugin(),
            APIPlugin(),
            SimpleTriggerPlugin(),
        ],
    )

    pgcraft_configure_metadata(metadata)
    return metadata
