"""cave's alembic Rewriter plugins for autogenerate.

Add new ``@cave_process_revision_directives.rewrites(...)`` handlers here
as further plugins are developed.
"""

from typing import TYPE_CHECKING

from alembic.autogenerate.rewriter import Rewriter
from alembic.operations import ops as alembic_ops

from cave.alembic.dependency import sort_migration_ops

if TYPE_CHECKING:
    from alembic.runtime.migration import MigrationContext
    from alembic.script.revision import _GetRevArg


cave_process_revision_directives = Rewriter()


@cave_process_revision_directives.rewrites(alembic_ops.UpgradeOps)
def _reorder_upgrade(
    _context: "MigrationContext",
    _revision: "_GetRevArg",
    upgrade_ops: alembic_ops.UpgradeOps,
) -> alembic_ops.UpgradeOps:
    upgrade_ops.ops[:] = sort_migration_ops(list(upgrade_ops.ops))
    return upgrade_ops


@cave_process_revision_directives.rewrites(alembic_ops.DowngradeOps)
def _reorder_downgrade(
    _context: "MigrationContext",
    _revision: "_GetRevArg",
    downgrade_ops: alembic_ops.DowngradeOps,
) -> alembic_ops.DowngradeOps:
    downgrade_ops.ops[:] = sort_migration_ops(
        list(downgrade_ops.ops),
        for_downgrade=True,
    )
    return downgrade_ops
