"""Monkey-patch for alembic_utils' entity comparison logic.

These patches are candidates for upstreaming to alembic_utils.

Problem
-------
``ReplaceableEntity.get_database_definition`` always simulates the entity
(temporarily creates it in the database inside a rolled-back transaction) to
obtain PostgreSQL's normalised form of the definition before comparison.

For entities that do not yet exist in the database — i.e. entities that will
receive a ``CreateOp`` — simulation fails if any dependency (e.g. a table the
view references) does not exist yet either.  This means ``just revision``
errors on a fresh database before any migrations have been applied.

Fix
---
Check whether the entity already exists in the database before simulating.
If it does not, return ``self`` directly.  No comparison is needed for a
create — the definition is used as-is.  Simulation still runs for existing
entities, preserving the normalised-comparison behaviour for ``ReplaceOp``
detection.

Upstream
--------
The fix is a two-line addition at the top of
``ReplaceableEntity.get_database_definition`` in
``alembic_utils/replaceable_entity.py``.
"""

from itertools import zip_longest

from alembic_utils.exceptions import UnreachableException
from alembic_utils.replaceable_entity import ReplaceableEntity
from alembic_utils.simulate import simulate_entity
from sqlalchemy.orm import Session


def _get_database_definition[T: ReplaceableEntity](
    self: T,
    sess: Session,
    dependencies: list[ReplaceableEntity] | None = None,
) -> T:
    """Return PostgreSQL's normalised form of this entity.

    If the entity does not yet exist in the database, returns ``self``
    directly to avoid simulating against missing dependencies.
    """
    entities_in_database: list[T] = self.from_database(sess, schema=self.schema)

    if not any(x.identity == self.identity for x in entities_in_database):
        return self

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
