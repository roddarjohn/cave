"""Alembic operations and comparators for managing PostgreSQL schemas."""

from alembic.autogenerate import comparators, renderers
from alembic.autogenerate.api import AutogenContext
from alembic.operations import MigrateOperation, Operations
from alembic.operations.ops import UpgradeOps
from sqlalchemy import MetaData, text

SYSTEM_SCHEMAS = {"public", "pg_catalog", "information_schema", "pg_toast"}


class CreateSchemaOp(MigrateOperation):
    """Alembic operation to create a PostgreSQL schema."""

    def __init__(self, schema_name: str) -> None:
        """Initialise with the name of the schema to create."""
        self.schema_name = schema_name

    @classmethod
    def create_schema(cls, operations: Operations, schema_name: str) -> None:
        """Invoke the create schema operation."""
        return operations.invoke(cls(schema_name))

    def reverse(self) -> "DropSchemaOp":
        """Return the inverse operation."""
        return DropSchemaOp(self.schema_name)


class DropSchemaOp(MigrateOperation):
    """Alembic operation to drop a PostgreSQL schema."""

    def __init__(self, schema_name: str) -> None:
        """Initialise with the name of the schema to drop."""
        self.schema_name = schema_name

    @classmethod
    def drop_schema(cls, operations: Operations, schema_name: str) -> None:
        """Invoke the drop schema operation."""
        return operations.invoke(cls(schema_name))

    def reverse(self) -> CreateSchemaOp:
        """Return the inverse operation."""
        return CreateSchemaOp(self.schema_name)


@Operations.implementation_for(CreateSchemaOp)
def create_schema(operations: Operations, operation: CreateSchemaOp) -> None:
    """Execute CREATE SCHEMA for the given operation."""
    operations.execute(f"CREATE SCHEMA IF NOT EXISTS {operation.schema_name}")


@Operations.implementation_for(DropSchemaOp)
def drop_schema(operations: Operations, operation: DropSchemaOp) -> None:
    """Execute DROP SCHEMA for the given operation."""
    operations.execute(f"DROP SCHEMA IF EXISTS {operation.schema_name}")


@comparators.dispatch_for("schema")
def compare_schemas(
    autogen_context: AutogenContext,
    upgrade_ops: UpgradeOps,
    _schemas: frozenset[str | None],
) -> None:
    """Compare desired schemas against the database and emit create/drop ops."""
    conn = autogen_context.connection
    if conn is None:
        return

    existing = {
        row[0]
        for row in conn.execute(
            text("SELECT schema_name FROM information_schema.schemata")
        )
    }

    raw_metadata = autogen_context.metadata
    if not isinstance(raw_metadata, MetaData):
        return
    metadata = raw_metadata

    desired = {
        table.schema
        for table in metadata.tables.values()
        if table.schema is not None
    }

    for schema in desired - existing - SYSTEM_SCHEMAS:
        # Should be replaced by uniform dependency handling
        upgrade_ops.ops.insert(
            0, CreateSchemaOp(schema)
        )  # prepend so it runs first

    for schema in existing - desired - SYSTEM_SCHEMAS:
        upgrade_ops.ops.append(DropSchemaOp(schema))


@renderers.dispatch_for(CreateSchemaOp)
def render_create_schema(
    _autogen_context: AutogenContext, op: CreateSchemaOp
) -> str:
    """Render a CreateSchemaOp as a migration script string."""
    return f"op.execute('CREATE SCHEMA IF NOT EXISTS {op.schema_name}')"


@renderers.dispatch_for(DropSchemaOp)
def render_drop_schema(
    _autogen_context: AutogenContext, op: DropSchemaOp
) -> str:
    """Render a DropSchemaOp as a migration script string."""
    return f"op.execute('DROP SCHEMA IF EXISTS {op.schema_name}')"
