"""Registration of cave's alembic extensions."""

from cave.alembic import renderer as _renderer
from cave.alembic import schema as _schema  # noqa: F401
from cave.alembic.rewriter import cave_process_revision_directives
from cave.patches import apply_all as _apply_all

__all__ = ["cave_alembic_hook", "cave_process_revision_directives"]


def cave_alembic_hook() -> None:
    """Register cave's alembic extensions."""
    _apply_all()
    _renderer.apply()
