from __future__ import annotations

import logging
from dataclasses import dataclass
from graphlib import TopologicalSorter
from typing import Literal

import pglast
import pglast.parser
from alembic.operations import ops as alembic_ops
from pglast.visitors import Visitor
from sqlalchemy import MetaData  # noqa: TC002
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
        for attr in ("view", "function", "procedure"):
            entity = getattr(op, attr, None)
            if entity is not None:
                return entity.schema or "public"

        trigger = getattr(op, "trigger", None)
        if trigger is not None:
            # PostgreSQL triggers store the target as "schema.table"
            # in the `on` attribute; they have no `schema` field.
            on_schema, _ = _parse_qualified_name(trigger.on)
            return f"__triggers__{on_schema}"

    return None


def _entity_name(op: AnyOp) -> str | None:
    """Extract the entity name from a declarative-extensions op."""
    for attr in ("view", "function", "procedure"):
        entity = getattr(op, attr, None)
        if entity is not None:
            return entity.name

    trigger = getattr(op, "trigger", None)
    if trigger is not None:
        return trigger.name
    return None


def _entity_definition(op: AnyOp) -> str | None:
    """Extract a parseable SQL definition (views only).

    Function/procedure bodies contain PL/pgSQL which needs special handling;
    use :func:`_plpgsql_table_refs` for those.
    """
    view = getattr(op, "view", None)
    if view is not None and hasattr(view, "definition"):
        defn = view.definition
        if isinstance(defn, str):
            return defn
    return None


def _plpgsql_queries(obj: object) -> list[str]:
    """Recursively collect SQL query strings from a ``parse_plpgsql`` tree."""
    queries: list[str] = []
    if isinstance(obj, dict):
        if "PLpgSQL_expr" in obj:
            query = obj["PLpgSQL_expr"].get("query", "")  # ty: ignore[invalid-argument-type, unresolved-attribute]
            # Skip trivial expressions like NEW/OLD.
            if query and query.upper() not in ("NEW", "OLD"):
                queries.append(query)
        else:
            for value in obj.values():
                queries.extend(_plpgsql_queries(value))
    elif isinstance(obj, list):
        for item in obj:
            queries.extend(_plpgsql_queries(item))
    return queries


def _function_return_refs(
    op: AnyOp,
) -> set[tuple[str, str]]:
    """Extract ``(schema, name)`` from a function's return type.

    Parses ``SETOF schema.name`` patterns from the ``returns``
    attribute of function/procedure ops.

    Returns an empty set for non-function ops or non-SETOF returns.
    """
    func = getattr(op, "function", None)
    if func is None:
        return set()

    returns = getattr(func, "returns", None)
    if not returns or not isinstance(returns, str):
        return set()

    # Match "SETOF schema.name" (case-insensitive).
    stripped = returns.strip()
    upper = stripped.upper()
    if not upper.startswith("SETOF "):
        return set()

    ref = stripped[6:].strip()
    schema, sep, name = ref.partition(".")
    if sep:
        return {(schema.lower(), name.lower())}
    return set()


def _sql_function_table_refs(
    op: AnyOp,
) -> set[tuple[str, str]]:
    """Extract ``(schema, table)`` pairs from a LANGUAGE sql function.

    Uses ``pglast.parse_sql`` to find table references in the
    function body.

    Returns an empty set for non-sql-language ops or if parsing
    fails.
    """
    func = getattr(op, "function", None)
    if func is None:
        return set()

    defn = getattr(func, "definition", None)
    language = getattr(func, "language", "")
    if not defn or language.lower() != "sql":
        return set()

    return _view_table_refs(defn)


def _plpgsql_table_refs(
    op: AnyOp,
) -> set[tuple[str, str]]:
    """Extract ``(schema, table)`` pairs from a PL/pgSQL function body.

    Uses ``pglast.parse_plpgsql`` to extract embedded SQL statements from
    the function body, then ``pglast.parse_sql`` to find table references
    within those statements.

    Returns an empty set for non-function ops or if parsing fails.
    """
    func = getattr(op, "function", None)
    if func is None:
        return set()

    defn = getattr(func, "definition", None)
    language = getattr(func, "language", "")
    if not defn or language.lower() != "plpgsql":
        return set()

    # pglast.parse_plpgsql requires a full CREATE FUNCTION statement.
    schema_part = f"{func.schema}." if func.schema else ""
    wrapper = (
        f"CREATE FUNCTION {schema_part}__cave_parse_helper()"
        f" RETURNS trigger LANGUAGE plpgsql AS $${defn}$$;"
    )

    try:
        tree = pglast.parse_plpgsql(wrapper)
    except pglast.parser.ParseError:  # ty: ignore[possibly-missing-attribute]
        logger.debug(
            "Could not parse PL/pgSQL body for %s",
            func.name,
        )
        return set()

    refs: set[tuple[str, str]] = set()
    for query in _plpgsql_queries(tree):
        try:

            class _TableFinder(Visitor):
                def visit_RangeVar(  # noqa: N802
                    self,
                    _ancestors: object,
                    node: object,
                ) -> None:
                    schema = getattr(node, "schemaname", None)
                    name = getattr(node, "relname", None)
                    if schema and name:
                        refs.add((schema.lower(), name.lower()))

            parsed = pglast.parse_sql(query)  # ty: ignore[possibly-missing-attribute]
            _TableFinder()(parsed)
        except pglast.parser.ParseError:  # ty: ignore[possibly-missing-attribute]
            logger.debug(
                "Could not parse embedded SQL in %s: %s",
                func.name,
                query[:80],
            )

    return refs


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


def _parse_qualified_name(
    qualified: str,
) -> tuple[str, str]:
    """Split ``schema.name`` into ``(schema, name)``."""
    parts = qualified.split(".", 1)
    if len(parts) > 1:
        return parts[0].lower(), parts[1].lower()
    return "public", parts[0].lower()


def _refs_for_trigger(
    op: AnyOp,
    phase: Phase | None,
) -> set[EntityIdentifier]:
    """Return refs for trigger ops (target view and executed function)."""
    trigger = getattr(op, "trigger", None)
    if trigger is None:
        return set()

    refs: set[EntityIdentifier] = set()

    # Depend on the target view/table (``on``).
    on_schema, on_name = _parse_qualified_name(trigger.on)
    refs.add(
        EntityIdentifier(
            schema=on_schema,
            name=on_name,
            phase=phase,
        )
    )
    refs.add(EntityIdentifier(schema=on_schema, phase=phase))

    # Depend on the executed function.
    fn_schema, fn_name = _parse_qualified_name(trigger.execute)
    refs.add(
        EntityIdentifier(
            schema=fn_schema,
            name=fn_name,
            phase=phase,
        )
    )

    return refs


def _add_table_refs(
    refs: set[EntityIdentifier],
    table_pairs: set[tuple[str, str]],
    phase: Phase | None,
) -> None:
    """Add ``EntityIdentifier`` entries for ``(schema, name)`` pairs."""
    for ref_schema, ref_name in table_pairs:
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


def _view_table_refs(definition: str) -> set[tuple[str, str]]:
    """Extract ``(schema, table)`` pairs from a view SQL definition."""
    refs: set[tuple[str, str]] = set()
    try:
        parsed = pglast.parse_sql(definition)  # ty: ignore[possibly-missing-attribute]

        class _TableFinder(Visitor):
            def visit_RangeVar(  # noqa: N802
                self,
                _ancestors: object,
                node: object,
            ) -> None:
                schema = getattr(node, "schemaname", None)
                name = getattr(node, "relname", None)
                if schema and name:
                    refs.add((schema.lower(), name.lower()))

        _TableFinder()(parsed)
    except pglast.parser.ParseError:  # ty: ignore[possibly-missing-attribute]
        logger.debug(
            "Could not parse view definition: %s",
            definition[:80],
        )
    return refs


def _refs_from_definitions(
    op: AnyOp,
    phase: Phase | None,
) -> set[EntityIdentifier]:
    """Extract entity refs from view SQL and function bodies/returns."""
    refs: set[EntityIdentifier] = set()

    # View definitions: parse with pglast.
    definition = _entity_definition(op)
    if definition is not None:
        _add_table_refs(refs, _view_table_refs(definition), phase)

    # PL/pgSQL function bodies: parse with pglast.
    _add_table_refs(refs, _plpgsql_table_refs(op), phase)

    # LANGUAGE sql function bodies: parse with pglast.
    _add_table_refs(refs, _sql_function_table_refs(op), phase)

    # Function return types: SETOF schema.view.
    _add_table_refs(refs, _function_return_refs(op), phase)

    return refs


def _refs_for_declarative_op(
    op: AnyOp,
    phase: Phase | None,
) -> set[EntityIdentifier]:
    """Return references for view/function/procedure/trigger ops."""
    # Triggers have their own reference logic via the `on` field.
    trigger_refs = _refs_for_trigger(op, phase)
    if trigger_refs:
        return trigger_refs

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

    refs |= _refs_from_definitions(op, phase)

    refs.discard(self_id)
    return refs


def build_fk_graph_from_metadata(
    metadata: MetaData,
) -> dict[tuple[str, str], set[tuple[str, str]]]:
    """Build a table FK dependency map from SQLAlchemy metadata.

    Uses the metadata's knowledge of foreign key relationships
    rather than parsing column objects from migration ops.
    """
    graph: dict[tuple[str, str], set[tuple[str, str]]] = {}

    for table in metadata.tables.values():
        key = (
            (table.schema or "public").lower(),
            table.name.lower(),
        )
        targets: set[tuple[str, str]] = set()
        for fk in table.foreign_keys:
            ref = fk.column.table
            targets.add(
                (
                    (ref.schema or "public").lower(),
                    ref.name.lower(),
                )
            )
        # Exclude self-references.
        targets.discard(key)
        if targets:
            graph[key] = targets
    return graph


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
    *,
    fk_graph: (dict[tuple[str, str], set[tuple[str, str]]] | None) = None,
) -> list[AnyOp]:
    """Return *migration_ops* topologically sorted by entity dependencies.

    Dependency edges are derived from the ops themselves:

    - A table depends on its schema.
    - A table depends on tables it references via foreign keys.
    - A replaceable entity (view, function, ...) depends on its schema
      and on every schema-qualified table or view referenced in its SQL
      definition.

    Only dependencies between ops in the current migration produce edges;
    references to already-existing objects are ignored.

    Edge direction is determined per-op by phase: drop-phase ops reverse
    their edges (dependents dropped first), create-phase ops use normal
    direction (dependencies created first).

    :param migration_ops: Operations from a single migration script.
    :param fk_graph: FK dependency map (table -> referenced tables),
        built from :func:`build_fk_graph_from_metadata`.
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

    if fk_graph is None:
        fk_graph = {}

    for current_id, current_op in op_by_entity.items():
        sorter.add(current_id)
        phase = _op_phase(current_op)

        refs = _entity_references(current_op)

        # Add FK-based refs for table ops.  For DropTableOp the
        # op itself has no column info, so we consult the FK
        # graph built from CreateTableOps.
        if isinstance(
            current_op,
            (
                alembic_ops.CreateTableOp,
                alembic_ops.DropTableOp,
            ),
        ):
            table_key = (
                (current_op.schema or "public").lower(),
                current_op.table_name.lower(),
            )
            for ref_schema, ref_table in fk_graph.get(table_key, set()):
                refs.add(
                    EntityIdentifier(
                        schema=ref_schema,
                        name=ref_table,
                        phase=phase,
                    )
                )

        for ref_id in refs:
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
