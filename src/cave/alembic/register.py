from sqlalchemy import MetaData
from sqlalchemy_declarative_extensions.alembic import register_alembic_events

from cave.alembic.renderer import register_renderers
from cave.alembic.rewriter import cave_process_revision_directives
from cave.alembic.schema import register_schemas
from cave.models.roles import register_roles
from cave.patches import apply_all

__all__ = [
    "cave_alembic_hook",
    "cave_configure_metadata",
    "cave_process_revision_directives",
]


def cave_alembic_hook() -> None:
    """Register cave's alembic extensions (call before importing models).

    Usage in ``env.py``::

        from cave.alembic.register import (
            cave_alembic_hook,
            cave_configure_metadata,
            cave_process_revision_directives,
        )

        cave_alembic_hook()

        # ... import models / build metadata ...

        cave_configure_metadata(target_metadata)

    Then pass ``cave_process_revision_directives`` to
    ``context.configure(process_revision_directives=...)``.
    """
    register_alembic_events()
    register_renderers()
    apply_all()


def cave_configure_metadata(metadata: MetaData) -> None:
    """Register schemas, roles, and grants on *metadata* (call after models)."""
    register_schemas(metadata)
    register_roles(metadata)
