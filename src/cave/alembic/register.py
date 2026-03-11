from cave.alembic import renderer as _renderer
from cave.alembic import schema as _schema  # noqa: F401
from cave.alembic.rewriter import cave_process_revision_directives
from cave.patches import apply_all as _apply_all

__all__ = ["cave_alembic_hook", "cave_process_revision_directives"]


def cave_alembic_hook() -> None:
    """Register cave's alembic extensions.

    Call this at the top of ``env.py`` and pass
    ``cave_process_revision_directives`` to ``process_revision_directives``::

        from cave.alembic import (
            cave_alembic_hook,
            cave_process_revision_directives,
        )

        cave_alembic_hook()

        def run_migrations_online() -> None:
            ...
            context.configure(
                ...,
                process_revision_directives=cave_process_revision_directives,
            )

    To chain with another rewriter::

        process_revision_directives=cave_process_revision_directives.chain(other),
    """
    _apply_all()
    _renderer.apply()
