from sqlalchemy import MetaData
from sqlalchemy_declarative_extensions.alembic import register_alembic_events

from pgcraft.alembic.renderer import register_renderers
from pgcraft.alembic.rewriter import pgcraft_process_revision_directives
from pgcraft.alembic.schema import register_schemas
from pgcraft.models.roles import register_roles
from pgcraft.patches import apply_all

__all__ = [
    "pgcraft_alembic_hook",
    "pgcraft_configure_metadata",
    "pgcraft_process_revision_directives",
]


def pgcraft_alembic_hook() -> None:
    """Register pgcraft's alembic extensions (call before importing models).

    Usage in ``env.py``::

        from pgcraft.alembic.register import (
            pgcraft_alembic_hook,
            pgcraft_configure_metadata,
            pgcraft_process_revision_directives,
        )

        pgcraft_alembic_hook()

        # ... import models / build metadata ...

        pgcraft_configure_metadata(target_metadata)

    Then pass ``pgcraft_process_revision_directives`` to
    ``context.configure(process_revision_directives=...)``.
    """
    register_alembic_events()
    register_renderers()
    apply_all()


def pgcraft_configure_metadata(metadata: MetaData) -> None:
    """Register schemas, roles, and grants on *metadata* (call after models)."""
    register_schemas(metadata)
    register_roles(metadata)
