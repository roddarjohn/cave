import hashlib
from collections.abc import Callable

from sqlalchemy import Table
from sqlalchemy.schema import Constraint, ForeignKeyConstraint


def _make_token(
    prefix: str,
    get_cols: Callable[[Constraint, Table], str],
    get_ref: Callable[[Constraint, Table], str] | None = None,
    max_length: int = 63,
) -> Callable[[Constraint, Table], str]:
    def token(constraint: Constraint, table: Table) -> str:
        cols = get_cols(constraint, table)
        ref = f"__{get_ref(constraint, table)}" if get_ref else ""
        full = f"{prefix}__{table.name}__{cols}{ref}".strip("_")

        if len(full) > max_length:
            digest = hashlib.md5(
                full.encode(), usedforsecurity=False
            ).hexdigest()[:8]
            full = f"{full[: max_length - 9]}_{digest}"

        return full

    return token


def _cols(constraint: Constraint, _table: Table) -> str:
    return "_".join(c.name for c in constraint.columns)


def _ref(constraint: ForeignKeyConstraint, _table: Table) -> str:
    return "_".join(f.column.table.name for f in constraint.elements)


def build_naming_convention(
    max_length: int = 63,
) -> dict[str, str | Callable[[Constraint, Table], str]]:
    """Build a SQLAlchemy naming convention dict with length-safe names."""
    return {
        # Custom token callables
        "fk_name": _make_token("fk", _cols, _ref, max_length=max_length),
        "uq_name": _make_token("uq", _cols, max_length=max_length),
        "ix_name": _make_token("ix", _cols, max_length=max_length),
        "pk_name": _make_token("pk", _cols, max_length=max_length),
        "ck_name": _make_token("ck", _cols, max_length=max_length),
        # Template strings that reference the token callables above
        "fk": "%(fk_name)s",
        "uq": "%(uq_name)s",
        "ix": "%(ix_name)s",
        "pk": "%(pk_name)s",
        "ck": "%(ck_name)s",
    }
