"""Monkey-patch for alembic_utils' entity comparison logic.

These patches are candidates for upstreaming to alembic_utils.

Problem
-------
``ReplaceableEntity.get_database_definition`` always simulates the entity
(temporarily creates it in the database inside a rolled-back transaction) to
obtain PostgreSQL's normalized form of the definition before comparison.

This fails in two cases:

1. **New entities** -- entities that will receive a ``CreateOp`` -- simulation
   fails if any dependency (e.g. a table the view references) does not exist
   yet.  This means ``alembic revision`` errors on a fresh database before
   any migrations have been applied.

2. **Existing entities with new dependencies** -- e.g. a view that is being
   updated to include a column that was just added to a table it references.
   The simulation tries to recreate the view with the new definition, but
   the new column doesn't exist in the database yet, so it fails.  This
   should produce a ``ReplaceOp``, not an error.

Fix
---
For case 1: check whether the entity already exists before simulating.
If it does not, return ``self`` directly -- no comparison is needed for a
create.

For case 2: if simulation fails for an entity that *does* exist, return
``self`` directly.  Since the Python definition has changed (it references
the new column), it will differ from the database version and correctly
trigger a ``ReplaceOp``.

Upstream
--------
A small addition to ``ReplaceableEntity.get_database_definition`` in
``alembic_utils/replaceable_entity.py``.
"""

import logging
from itertools import zip_longest

from alembic_utils.exceptions import UnreachableException
from alembic_utils.replaceable_entity import ReplaceableEntity
from alembic_utils.simulate import simulate_entity
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def _get_database_definition[T: ReplaceableEntity](
    self: T,
    sess: Session,
    dependencies: list[ReplaceableEntity] | None = None,
) -> T:
    """Return PostgreSQL's normalized form of this entity.

    If the entity does not yet exist in the database, returns ``self``
    directly to avoid simulating against missing dependencies.

    If simulation fails for an existing entity (e.g. because the new
    definition references a column that doesn't exist yet), returns
    ``self`` so the difference triggers a ``ReplaceOp``.
    """
    entities_in_database: list[T] = self.from_database(sess, schema=self.schema)

    if not any(x.identity == self.identity for x in entities_in_database):
        return self

    # Simulation can fail when the new definition references objects that
    # don't exist in the database yet (e.g. a column being added in the
    # same migration).  Returning `self` makes the Python definition differ
    # from the database version, which correctly triggers a ReplaceOp.
    try:
        with simulate_entity(sess, self, dependencies) as simul_sess:
            simul_sess.execute(self.to_sql_statement_drop())
            db_entities: list[T] = sorted(
                self.from_database(simul_sess, schema=self.schema),
                key=lambda x: x.identity,
            )

        with simulate_entity(sess, self, dependencies) as simul_sess:
            all_w_self: list[T] = sorted(
                self.from_database(simul_sess, schema=self.schema),
                key=lambda x: x.identity,
            )
    except SQLAlchemyError:
        logger.info(
            "Simulation failed for existing entity %s; "
            "assuming definition changed (will produce ReplaceOp)",
            self.identity,
        )
        return self

    for without_self, with_self in zip_longest(db_entities, all_w_self):
        if without_self is None or without_self.identity != with_self.identity:
            return with_self

    raise UnreachableException


class ComparatorPatch:
    """Patch for ``ReplaceableEntity.get_database_definition``."""

    @staticmethod
    def apply() -> None:
        """Apply the patch."""
        ReplaceableEntity.get_database_definition = _get_database_definition  # type: ignore[method-assign]
