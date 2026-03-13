from typing import TYPE_CHECKING, cast

from alembic.autogenerate.rewriter import Rewriter
from alembic.operations import ops as alembic_ops

from pgcraft.alembic.dependency import (
    AnyOp,
    build_fk_graph_from_metadata,
    expand_update_ops,
    sort_migration_ops,
)

if TYPE_CHECKING:
    from alembic.runtime.migration import MigrationContext
    from alembic.script.revision import _GetRevArg


pgcraft_process_revision_directives = Rewriter()


def _sort_ops(
    context: "MigrationContext",
    ops: list[alembic_ops.MigrateOperation],
) -> list[alembic_ops.MigrateOperation]:
    """Expand update ops and sort by dependency order."""
    metadata = context.opts.get("target_metadata")
    fk_graph = (
        build_fk_graph_from_metadata(metadata) if metadata is not None else {}
    )
    expanded = expand_update_ops(cast("list[AnyOp]", ops))
    return cast(
        "list[alembic_ops.MigrateOperation]",
        sort_migration_ops(expanded, fk_graph=fk_graph),
    )


@pgcraft_process_revision_directives.rewrites(alembic_ops.UpgradeOps)
def _reorder_upgrade(
    context: "MigrationContext",
    _revision: "_GetRevArg",
    upgrade_ops: alembic_ops.UpgradeOps,
) -> alembic_ops.UpgradeOps:
    upgrade_ops.ops[:] = _sort_ops(context, list(upgrade_ops.ops))
    return upgrade_ops


@pgcraft_process_revision_directives.rewrites(alembic_ops.DowngradeOps)
def _reorder_downgrade(
    context: "MigrationContext",
    _revision: "_GetRevArg",
    downgrade_ops: alembic_ops.DowngradeOps,
) -> alembic_ops.DowngradeOps:
    downgrade_ops.ops[:] = _sort_ops(context, list(downgrade_ops.ops))
    return downgrade_ops
