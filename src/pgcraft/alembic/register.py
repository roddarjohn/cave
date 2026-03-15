from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy_declarative_extensions.alembic import (
    register_alembic_events,
)

from pgcraft.alembic.renderer import register_renderers
from pgcraft.alembic.rewriter import (
    pgcraft_process_revision_directives,
)
from pgcraft.alembic.schema import register_schemas
from pgcraft.patches import apply_all

if TYPE_CHECKING:
    from sqlalchemy import MetaData

    from pgcraft.config import PGCraftConfig

__all__ = [
    "pgcraft_alembic_hook",
    "pgcraft_configure_metadata",
    "pgcraft_process_revision_directives",
]


def pgcraft_alembic_hook(
    config: PGCraftConfig | None = None,
) -> None:
    """Register pgcraft's alembic extensions.

    Call before importing models.

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

    Args:
        config: Optional config providing extensions
            whose ``configure_alembic()`` hooks will be
            called.

    """
    # This is from sqlalchemy-declarative-extensions
    register_alembic_events()

    # These are pgcraft specific
    register_renderers()
    apply_all()

    if config is not None:
        for ext in config._resolved_extensions():  # noqa: SLF001
            ext.configure_alembic()


def pgcraft_configure_metadata(
    metadata: MetaData,
    config: PGCraftConfig | None = None,
) -> None:
    """Register schemas and extension hooks on *metadata*.

    Args:
        metadata: The SQLAlchemy ``MetaData`` to configure.
        config: Optional config providing extensions.
            If ``None``, falls back to
            ``metadata.info["pgcraft_config"]``.

    """
    register_schemas(metadata)
    cfg = config or metadata.info.get("pgcraft_config")
    if cfg is not None:
        for ext in cfg._resolved_extensions():  # noqa: SLF001
            ext.configure_metadata(metadata)
