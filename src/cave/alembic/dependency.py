import logging
from dataclasses import dataclass
from graphlib import TopologicalSorter
from typing import Literal

import sqlglot
from alembic.operations import ops as alembic_ops
from alembic_utils.reversible_op import (
    CreateOp,
    DropOp,
    ReplaceOp,
    ReversibleOp,
    RevertOp,
)
from sqlglot.expressions import Table as SqlglotTable

from cave.alembic.schema import CreateSchemaOp, DropSchemaOp

logger = logging.getLogger(__name__)

Phase = Literal["drop", "create"]


@dataclass(frozen=True)
class EntityIdentifier:
    """Identifies a database entity within a migration's dependency graph.

    ``name`` is ``None`` for schema-level entities (i.e. the entity *is*
    the schema).  For tables, views, and functions, ``name`` holds the
    unqualified object name and ``schema`` holds its containing schema.

    ``phase`` distinguishes drop and create ops for the same entity when
    a ``ReplaceOp`` has been expanded.  ``"drop"`` ops are ordered before
    ``"create"`` ops for the same entity.
    """

    schema: str = "public"
    name: str | None = None
    phase: Phase | None = None


def _op_phase(op: alembic_ops.MigrateOperation) -> Phase | None:
    """Return ``"drop"`` or ``"create"`` for ops that have a direction.

    Returns ``None`` for ``ModifyTableOps`` (column adds/drops) and any
    ``ReplaceOp``/``RevertOp`` that wasn't split by ``expand_replace_ops``.
    """
    if isinstance(op, (DropSchemaOp, alembic_ops.DropTableOp, DropOp)):
        return "drop"

    if isinstance(op, (CreateSchemaOp, alembic_ops.CreateTableOp, CreateOp)):
        return "create"

    return None


def _entity_identifier(
    op: alembic_ops.MigrateOperation,
) -> EntityIdentifier | None:
    """Return the identifier for the entity this op acts on.

    Returns ``None`` for ops whose ordering relative to others is
    unconstrained (e.g. column modifications on existing tables).
    """
    phase = _op_phase(op)

    if isinstance(op, (CreateSchemaOp, DropSchemaOp)):
        return EntityIdentifier(schema=op.schema_name.lower(), phase=phase)

    if isinstance(op, (alembic_ops.CreateTableOp, alembic_ops.DropTableOp)):
        return EntityIdentifier(
            schema=(op.schema or "public").lower(),
            name=op.table_name.lower(),
            phase=phase,
        )

    if isinstance(op, alembic_ops.ModifyTableOps):
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
            phase=phase,
        )

    logger.warning(
        "Unhandled op type %s; ordering is unconstrained", type(op).__name__
    )
    return None


def _entity_references(
    op: alembic_ops.MigrateOperation,
) -> set[EntityIdentifier]:
    """Return identifiers of entities this op references.

    The sort loop decides edge direction based on the op's phase:
    create-phase ops need their references to exist first (normal edge),
    drop-phase ops need their dependents dropped first (reversed edge).

    Only identifiers present among the current migration's ops will
    produce dependency edges; references to already-existing entities
    are filtered out in ``sort_migration_ops``.
    """
    phase = _op_phase(op)

    if isinstance(op, (alembic_ops.CreateTableOp, alembic_ops.DropTableOp)):
        return {
            EntityIdentifier(
                schema=(op.schema or "public").lower(),
                phase=phase,
            )
        }

    if isinstance(op, ReversibleOp):
        self_schema = op.target.schema.lower()
        base_name = op.target.signature.lower().split("(")[0].strip()
        self_id = EntityIdentifier(
            schema=self_schema,
            name=base_name,
            phase=phase,
        )

        refs: set[EntityIdentifier] = set()

        if phase == "create":
            # Must drop ourselves before recreating.
            refs.add(
                EntityIdentifier(
                    schema=self_schema,
                    name=base_name,
                    phase="drop",
                )
            )

        # Schema reference.
        if phase != "drop":
            refs.add(
                EntityIdentifier(
                    schema=self_schema,
                    phase=phase,
                )
            )

        # SQL-level references to tables and views.
        if hasattr(op.target, "definition"):
            ast = sqlglot.parse_one(op.target.definition, dialect="postgres")
            for table_ref in ast.find_all(SqlglotTable):
                if table_ref.db:
                    ref_schema = table_ref.db.lower()
                    ref_name = table_ref.name.lower()
                    refs.add(
                        EntityIdentifier(
                            schema=ref_schema,
                            name=ref_name,
                            phase=phase,
                        )
                    )
                    # Also reference any ModifyTableOps (phase=None) for
                    # the same table, so column changes are ordered
                    # correctly relative to view drops/creates.
                    if phase is not None:
                        refs.add(
                            EntityIdentifier(
                                schema=ref_schema,
                                name=ref_name,
                            )
                        )

        refs.discard(self_id)
        return refs

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


def expand_replace_ops(
    migration_ops: list[alembic_ops.MigrateOperation],
) -> list[alembic_ops.MigrateOperation]:
    """Split ``ReplaceOp`` and ``RevertOp`` into ``DropOp`` + ``CreateOp``.

    ``CREATE OR REPLACE VIEW`` fails when another view that depends on this
    one has an incompatible column list.  Splitting into separate drop/create
    operations lets the topological sort interleave them correctly: drop
    dependents first, then drop and recreate dependencies, then recreate
    dependents.
    """
    result: list[alembic_ops.MigrateOperation] = []
    for op in migration_ops:
        if isinstance(op, ReplaceOp):
            result.append(DropOp(op.target))
            result.append(CreateOp(op.target))

        elif isinstance(op, RevertOp):
            old_target = op.target._version_to_replace  # noqa: SLF001
            result.append(DropOp(op.target))

            if old_target is not None:
                result.append(CreateOp(old_target))

            else:
                result.append(CreateOp(op.target))

        else:
            result.append(op)

    return result


def sort_migration_ops(
    migration_ops: list[alembic_ops.MigrateOperation],
) -> list[alembic_ops.MigrateOperation]:
    """Return *migration_ops* topologically sorted by entity dependencies.

    Dependency edges are derived from the ops themselves:

    - A table depends on its schema.
    - A replaceable entity (view, function, …) depends on its schema and on
      every schema-qualified table or view referenced in its SQL definition.

    Only dependencies between ops in the current migration produce edges;
    references to already-existing objects are ignored.

    Edge direction is determined per-op by phase: drop-phase ops reverse
    their edges (dependents dropped first), create-phase ops use normal
    direction (dependencies created first).

    :param migration_ops: Operations from a single migration script.
    :returns: A new list containing the same ops in dependency order.
    """
    logger.debug(
        "Sorting %d ops: %s",
        len(migration_ops),
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
        phase = _op_phase(current_op)

        for ref_id in _entity_references(current_op):
            ref_op = op_by_entity.get(ref_id)

            # Only add edges for references that resolve to an op in
            # this migration.  Unresolved references point to entities
            # that already exist in the database.
            if ref_op is not None:
                # Drop-phase: reverse the edge — dependents must be
                # dropped first.  Create-phase/unphased: normal
                # direction — the referenced entity must exist before
                # we use it.
                if phase == "drop":
                    node, prerequisite = ref_op, current_op

                else:
                    node, prerequisite = current_op, ref_op

                logger.debug(
                    "Edge: %s before %s",
                    _op_label(prerequisite),
                    _op_label(node),
                )
                sorter.add(node, prerequisite)

    sorted_ops = list(sorter.static_order())

    logger.debug(
        "Sorted order: %s",
        [_op_label(op) for op in sorted_ops],
    )

    return sorted_ops
