from sqlalchemy_declarative_extensions.alembic import register_alembic_events

from cave.alembic.renderer import register_renderers
from cave.alembic.rewriter import cave_process_revision_directives
from cave.patches import view_render

__all__ = ["cave_alembic_hook", "cave_process_revision_directives"]


def cave_alembic_hook() -> None:
    """Register cave's alembic extensions.

    Call this at the top of ``env.py`` and pass
    ``cave_process_revision_directives`` to ``process_revision_directives``::

        from cave.alembic.register import (
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
    register_alembic_events()
    register_renderers()
    view_render.apply()
