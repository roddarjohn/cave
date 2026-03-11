import logging
from dataclasses import dataclass
from graphlib import TopologicalSorter
from typing import Literal

import sqlglot
from alembic.operations import ops as alembic_ops
from sqlalchemy_declarative_extensions.alembic.function import (
    CreateFunctionOp,
    DropFunctionOp,
    UpdateFunctionOp,
)
from sqlalchemy_declarative_extensions.alembic.procedure import (
    CreateProcedureOp,
    DropProcedureOp,
    UpdateProcedureOp,
)
from sqlalchemy_declarative_extensions.alembic.schema import (
    CreateSchemaOp,
    DropSchemaOp,
)
from sqlalchemy_declarative_extensions.alembic.trigger import (
    CreateTriggerOp,
    DropTriggerOp,
    UpdateTriggerOp,
)
from sqlalchemy_declarative_extensions.alembic.view import (
    CreateViewOp,
    DropViewOp,
    UpdateViewOp,
)
from sqlalchemy_declarative_extensions.dialects.postgresql.grant import (
    DefaultGrantStatement,
    GrantStatement,
)
from sqlalchemy_declarative_extensions.grant.compare import (
    GrantPrivilegesOp,
    RevokePrivilegesOp,
)
from sqlalchemy_declarative_extensions.op import MigrateOp
from sqlalchemy_declarative_extensions.role.compare import (
    CreateRoleOp,
    DropRoleOp,
)
from sqlalchemy_declarative_extensions.role.generic import Role
from sqlglot.expressions import Table as SqlglotTable

logger = logging.getLogger(__name__)

# Union of alembic's built-in ops and sqlalchemy-declarative-extensions ops,
# which don't share a common base class.
AnyOp = alembic_ops.MigrateOperation | MigrateOp

Phase = Literal["drop", "create"]

# Op types grouped by direction.
_CREATE_OPS = (
    CreateRoleOp,
    CreateSchemaOp,
    alembic_ops.CreateTableOp,
    CreateViewOp,
    CreateFunctionOp,
    CreateProcedureOp,
    CreateTriggerOp,
    GrantPrivilegesOp,
)
_DROP_OPS = (
    DropRoleOp,
    DropSchemaOp,
    alembic_ops.DropTableOp,
    DropViewOp,
    DropFunctionOp,
    DropProcedureOp,
    DropTriggerOp,
    RevokePrivilegesOp,
)
_UPDATE_OPS = (
    UpdateViewOp,
    UpdateFunctionOp,
    UpdateProcedureOp,
    UpdateTriggerOp,
)


@dataclass(frozen=True)
class EntityIdentifier:
    """Identifies a database entity within a migration's dependency graph.

    ``name`` is ``None`` for schema-level entities (i.e. the entity *is*
    the schema).  For tables, views, and functions, ``name`` holds the
    unqualified object name and ``schema`` holds its containing schema.

    ``phase`` distinguishes drop and create ops for the same entity when
    an ``Update*Op`` has been expanded.  ``"drop"`` ops are ordered before
    ``"create"`` ops for the same entity.
    """

    schema: str = "public"
    name: str | None = None
    phase: Phase | None = None


def _op_phase(op: AnyOp) -> Phase | None:
    """Return ``"drop"`` or ``"create"`` for ops that have a direction.

    Returns ``None`` for ``ModifyTableOps`` (column adds/drops) and any
    ``Update*Op`` that wasn't split by ``expand_update_ops``.
    """
    if isinstance(op, _DROP_OPS):
        return "drop"

    if isinstance(op, _CREATE_OPS):
        return "create"

    return None


def _entity_schema(op: AnyOp) -> str | None:
    """Extract the schema name from an entity op."""
    if isinstance(op, (CreateSchemaOp, DropSchemaOp)):
        return op.schema.name

    if isinstance(op, _CREATE_OPS + _DROP_OPS):
        # View/Function/Procedure/Trigger ops store the entity
        # as the first dataclass field.
        for attr in ("view", "function", "procedure", "trigger"):
            entity = getattr(op, attr, None)
            if entity is not None:
                return entity.schema or "public"

    return None


def _entity_name(op: AnyOp) -> str | None:
    """Extract the entity name from a declarative-extensions op."""
    for attr in ("view", "function", "procedure", "trigger"):
        entity = getattr(op, attr, None)
        if entity is not None:
            return entity.name
    return None


def _entity_definition(op: AnyOp) -> str | None:
    """Extract the SQL definition from a declarative-extensions op."""
    for attr in ("view", "function", "procedure", "trigger"):
        entity = getattr(op, attr, None)
        if entity is not None and hasattr(entity, "definition"):
            defn = entity.definition
            if isinstance(defn, str):
                return defn
    return None


def _role_name(member: Role | str) -> str:
    """Extract a role name from a Role object or string."""
    if isinstance(member, Role):
        return member.name
    return member


def _id_for_declarative_op(
    op: AnyOp,
    phase: Phase | None,
) -> EntityIdentifier | None:
    """Return identifier for view/function/procedure/trigger ops."""
    name = _entity_name(op)
    if name is not None:
        schema = _entity_schema(op) or "public"
        return EntityIdentifier(
            schema=schema.lower(),
            name=name.lower(),
            phase=phase,
        )
    return None


def _entity_identifier(  # noqa: PLR0911
    op: AnyOp,
) -> EntityIdentifier | None:
    """Return the identifier for the entity this op acts on.

    Returns ``None`` for unrecognised op types (logged as a warning).
    """
    phase = _op_phase(op)

    if isinstance(op, (CreateSchemaOp, DropSchemaOp)):
        return EntityIdentifier(
            schema=op.schema.name.lower(),
            phase=phase,
        )

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

    if isinstance(op, (CreateRoleOp, DropRoleOp)):
        return EntityIdentifier(
            schema="__roles__",
            name=op.role.name.lower(),
            phase=phase,
        )

    if isinstance(op, (GrantPrivilegesOp, RevokePrivilegesOp)):
        return EntityIdentifier(
            schema="__grants__",
            name=str(op.to_sql()).lower(),
            phase=phase,
        )

    result = _id_for_declarative_op(op, phase)
    if result is not None:
        return result

    logger.warning(
        "Unhandled op type %s; ordering is unconstrained",
        type(op).__name__,
    )
    return None


def _refs_for_role(
    op: CreateRoleOp | DropRoleOp,
    phase: Phase | None,
) -> set[EntityIdentifier]:
    """Return references for a role op (member roles via in_roles)."""
    if not op.role.in_roles:
        return set()
    return {
        EntityIdentifier(
            schema="__roles__",
            name=_role_name(member).lower(),
            phase=phase,
        )
        for member in op.role.in_roles
    }


def _refs_for_grant(
    op: GrantPrivilegesOp | RevokePrivilegesOp,
    phase: Phase | None,
) -> set[EntityIdentifier]:
    """Return references for a grant/revoke op (target role + schemas)."""
    grant_obj = op.grant
    refs: set[EntityIdentifier] = set()

    # Depend on the target role.
    refs.add(
        EntityIdentifier(
            schema="__roles__",
            name=grant_obj.grant.target_role.lower(),
            phase=phase,
        )
    )

    # Depend on referenced schemas and target objects.
    if isinstance(grant_obj, GrantStatement):
        for target_name in grant_obj.targets:
            schema_part, sep, obj_name = target_name.partition(".")
            if sep:
                refs.add(
                    EntityIdentifier(
                        schema=schema_part.lower(),
                        phase=phase,
                    )
                )
                refs.add(
                    EntityIdentifier(
                        schema=schema_part.lower(),
                        name=obj_name.lower(),
                        phase=phase,
                    )
                )
            else:
                refs.add(
                    EntityIdentifier(
                        schema=target_name.lower(),
                        phase=phase,
                    )
                )
    elif isinstance(grant_obj, DefaultGrantStatement):
        for schema_name in grant_obj.default_grant.in_schemas:
            refs.add(
                EntityIdentifier(
                    schema=schema_name.lower(),
                    phase=phase,
                )
            )

    return refs


def _refs_for_declarative_op(
    op: AnyOp,
    phase: Phase | None,
) -> set[EntityIdentifier]:
    """Return references for view/function/procedure/trigger ops."""
    name = _entity_name(op)
    if name is None:
        return set()

    schema = (_entity_schema(op) or "public").lower()
    self_id = EntityIdentifier(
        schema=schema,
        name=name.lower(),
        phase=phase,
    )

    refs: set[EntityIdentifier] = set()

    if phase == "create":
        refs.add(
            EntityIdentifier(
                schema=schema,
                name=name.lower(),
                phase="drop",
            )
        )

    if phase != "drop":
        refs.add(EntityIdentifier(schema=schema, phase=phase))

    definition = _entity_definition(op)
    if definition is not None:
        ast = sqlglot.parse_one(definition, dialect="postgres")
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
                if phase is not None:
                    refs.add(
                        EntityIdentifier(
                            schema=ref_schema,
                            name=ref_name,
                        )
                    )

    refs.discard(self_id)
    return refs


def _entity_references(
    op: AnyOp,
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

    if isinstance(op, (CreateRoleOp, DropRoleOp)):
        return _refs_for_role(op, phase)

    if isinstance(op, (GrantPrivilegesOp, RevokePrivilegesOp)):
        return _refs_for_grant(op, phase)

    return _refs_for_declarative_op(op, phase)


def _op_label(op: AnyOp) -> str:
    """Return a compact label for an op, used in log messages.

    Format is ``OpType(schema.name)`` e.g. ``CreateViewOp(private.students)``,
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


def expand_update_ops(
    migration_ops: list[AnyOp],
) -> list[AnyOp]:
    """Split ``Update*Op`` into ``Drop*Op`` + ``Create*Op``.

    ``CREATE OR REPLACE VIEW`` fails when another view that depends on
    this one has an incompatible column list.  Splitting into separate
    drop/create operations lets the topological sort interleave them
    correctly: drop dependents first, then drop and recreate
    dependencies, then recreate dependents.
    """
    result: list[AnyOp] = []
    for op in migration_ops:
        if isinstance(op, UpdateViewOp):
            result.append(DropViewOp(op.from_view))
            result.append(CreateViewOp(op.view))

        elif isinstance(op, UpdateFunctionOp):
            result.append(DropFunctionOp(op.from_function))
            result.append(CreateFunctionOp(op.function))

        elif isinstance(op, UpdateProcedureOp):
            result.append(DropProcedureOp(op.from_procedure))
            result.append(CreateProcedureOp(op.procedure))

        elif isinstance(op, UpdateTriggerOp):
            result.append(DropTriggerOp(op.from_trigger))
            result.append(CreateTriggerOp(op.trigger))

        else:
            result.append(op)

    return result


def sort_migration_ops(
    migration_ops: list[AnyOp],
) -> list[AnyOp]:
    """Return *migration_ops* topologically sorted by entity dependencies.

    Dependency edges are derived from the ops themselves:

    - A table depends on its schema.
    - A replaceable entity (view, function, ...) depends on its schema
      and on every schema-qualified table or view referenced in its SQL
      definition.

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

    # Build a mapping from entity identifier to op.  Ops without a
    # recognised identifier are appended at the end in original order.
    op_by_entity: dict[EntityIdentifier, AnyOp] = {}
    unkeyed_ops: list[AnyOp] = []
    for op in migration_ops:
        entity = _entity_identifier(op)
        if entity is not None:
            op_by_entity[entity] = op
        else:
            unkeyed_ops.append(op)

    # Use EntityIdentifier (frozen dataclass, hashable) as graph nodes.
    sorter: TopologicalSorter[EntityIdentifier] = TopologicalSorter()

    for current_id, current_op in op_by_entity.items():
        sorter.add(current_id)
        phase = _op_phase(current_op)

        for ref_id in _entity_references(current_op):
            # Only add edges for references that resolve to an op in
            # this migration.  Unresolved references point to entities
            # that already exist in the database.
            if ref_id in op_by_entity:
                # Drop-phase: reverse the edge — dependents must be
                # dropped first.  Create-phase/unphased: normal
                # direction — the referenced entity must exist before
                # we use it.
                if phase == "drop":
                    node, prerequisite = ref_id, current_id
                else:
                    node, prerequisite = current_id, ref_id

                logger.debug(
                    "Edge: %s before %s",
                    _op_label(op_by_entity[prerequisite]),
                    _op_label(op_by_entity[node]),
                )
                sorter.add(node, prerequisite)

    sorted_ops = [
        op_by_entity[eid] for eid in sorter.static_order()
    ] + unkeyed_ops

    logger.debug(
        "Sorted order: %s",
        [_op_label(op) for op in sorted_ops],
    )

    return sorted_ops
