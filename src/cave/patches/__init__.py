from collections.abc import Callable

from cave.patches.alembic_utils.comparator import (
    ComparatorPatch as _ComparatorPatch,
)

_registry: list[Callable[[], None]] = [
    _ComparatorPatch.apply,
]


def apply_all() -> None:
    """Apply all registered patches."""
    for apply in _registry:
        apply()
