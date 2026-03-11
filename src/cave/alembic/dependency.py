"""Dependency-ordered rewriting of migration operations.

Operations within a single migration are topologically sorted so that each
entity is created after everything it depends on, and dropped before
everything it depends on.

Dependency edges are derived dynamically:

- A table depends on its schema (``CreateSchemaOp`` → ``CreateTableOp``).
- A replaceable entity (view, function, …) depends on its schema and on
  every schema-qualified table or view referenced in its SQL definition.

For downgrade the same graph is traversed in reverse: dependents are
dropped before their dependencies.

Usage
-----
Pass ``cave_process_revision_directives`` to
``process_revision_directives`` in ``env.py``::

    context.configure(
        ...,
        process_revision_directives=cave_process_revision_directives,
    )

Or chain it with another rewriter::

    context.configure(
        ...,
        process_revision_directives=cave_process_revision_directives.chain(other),
    )
"""

import logging
from dataclasses import dataclass
from graphlib import TopologicalSorter

import sqlglot
from alembic.operations import ops as alembic_ops
from alembic_utils.reversible_op import ReversibleOp
from sqlglot.expressions import Table as SqlglotTable

from cave.alembic.schema import CreateSchemaOp, DropSchemaOp

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EntityIdentifier:
    """Identifies a database entity within a migration's dependency graph.

    ``name`` is ``None`` for schema-level entities (i.e. the entity *is*
    the schema).  For tables, views, and functions, ``name`` holds the
    unqualified object name and ``schema`` holds its containing schema.
    """

    schema: str
    name: str | None = None


def _entity_identifier(
    op: alembic_ops.MigrateOperation,
) -> EntityIdentifier | None:
    """Return the identifier for the entity this op acts on.

    Returns ``None`` for ops whose ordering relative to others is
    unconstrained (e.g. column modifications on existing tables).
    """
    if isinstance(op, (CreateSchemaOp, DropSchemaOp)):
        return EntityIdentifier(schema=op.schema_name.lower())

    if isinstance(op, (alembic_ops.CreateTableOp, alembic_ops.DropTableOp)):
        return EntityIdentifier(
            schema=(op.schema or "public").lower(),
            name=op.table_name.lower(),
        )

    if isinstance(op, ReversibleOp):
        # Strip the argument list from function signatures like
        # "my_func(integer, text)" to get the bare name.
        base_name = op.target.signature.lower().split("(")[0].strip()
        return EntityIdentifier(
            schema=op.target.schema.lower(),
            name=base_name,
        )

    return None


def _entity_dependencies(
    op: alembic_ops.MigrateOperation,
) -> set[EntityIdentifier]:
    """Return identifiers of entities this op's entity depends on.

    Only identifiers present among the current migration's ops will
    produce dependency edges; references to already-existing entities
    are filtered out in ``_sort_migration_ops``.
    """
    if isinstance(op, (alembic_ops.CreateTableOp, alembic_ops.DropTableOp)):
        # A table depends only on its containing schema.
        return {EntityIdentifier(schema=(op.schema or "public").lower())}

    if isinstance(op, ReversibleOp):
        dependencies = {EntityIdentifier(schema=op.target.schema.lower())}
        if hasattr(op.target, "definition"):
            # Parse the SQL definition to find schema-qualified table and
            # view references.  sqlglot's Table expression covers both FROM
            # and JOIN clauses; ``table_ref.db`` is sqlglot's term for the
            # schema qualifier (the identifier before the dot).
            ast = sqlglot.parse_one(op.target.definition, dialect="postgres")

            for table_ref in ast.find_all(SqlglotTable):
                if table_ref.db:
                    dependencies.add(
                        EntityIdentifier(
                            schema=table_ref.db.lower(),
                            name=table_ref.name.lower(),
                        )
                    )
        return dependencies

    return set()


def _op_label(op: alembic_ops.MigrateOperation) -> str:
    """Return a compact label for an op, used in log messages.

    Format is ``OpType(schema.name)`` e.g. ``CreateOp(private.students)``,
    or ``OpType(schema)`` for schema-level ops.
    """
    identifier = _entity_identifier(op)
    if identifier is None:
        entity = "?"
    elif identifier.name is None:
        entity = identifier.schema
    else:
        entity = f"{identifier.schema}.{identifier.name}"
    return f"{type(op).__name__}({entity})"


def sort_migration_ops(
    migration_ops: list[alembic_ops.MigrateOperation],
    *,
    for_downgrade: bool = False,
) -> list[alembic_ops.MigrateOperation]:
    """Return *migration_ops* topologically sorted by entity dependencies.

    When *for_downgrade* is ``True``, the sort is reversed so that
    dependents are dropped before their dependencies.
    """
    direction = "downgrade" if for_downgrade else "upgrade"
    logger.debug(
        "Sorting %d ops for %s: %s",
        len(migration_ops),
        direction,
        [_op_label(op) for op in migration_ops],
    )

    op_by_entity: dict[EntityIdentifier, alembic_ops.MigrateOperation] = {
        entity: op
        for op in migration_ops
        if (entity := _entity_identifier(op)) is not None
    }

    sorter: TopologicalSorter[alembic_ops.MigrateOperation] = (
        TopologicalSorter()
    )

    for current_op in migration_ops:
        sorter.add(current_op)
        for dependency_identifier in _entity_dependencies(current_op):
            dependency_op = op_by_entity.get(dependency_identifier)

            if dependency_op is None:
                # This dependency is not being created in this migration
                # (it already exists in the database) — no edge needed.
                pass

            elif dependency_op is current_op:
                # Self-reference (e.g. a recursive CTE); no edge needed.
                pass

            elif for_downgrade:
                # Drop the dependent entity before the entity it depends on.
                logger.debug(
                    "Edge: %s before %s (drop dependent first)",
                    _op_label(current_op),
                    _op_label(dependency_op),
                )
                sorter.add(dependency_op, current_op)

            else:
                # Create the dependency before the entity that needs it.
                logger.debug(
                    "Edge: %s before %s (create dependency first)",
                    _op_label(dependency_op),
                    _op_label(current_op),
                )
                sorter.add(current_op, dependency_op)

    sorted_ops = list(sorter.static_order())
    logger.debug(
        "Sorted order: %s",
        [_op_label(op) for op in sorted_ops],
    )
    return sorted_ops
