"""Storable configuration schema for user-defined dimension tables.

:class:`DimensionConfig` is the contract between the API (which stores it)
and the runtime pipeline (which builds and applies schema from it).  It is
intentionally narrow:

- The column type vocabulary is a fixed allowlist so nothing other than
  well-understood PostgreSQL types can be declared.
- The table name is validated as a safe PostgreSQL identifier.
- The plugin system is hidden behind ``table_type`` and ``pk`` fields so
  stored configs remain stable even as pgcraft's internals evolve.
- A ``version`` field is included from the start so the format can be
  evolved without ambiguity.
"""

from __future__ import annotations

import re
from typing import Annotated, Literal

from pydantic import BaseModel, StringConstraints, field_validator

# Postgres identifiers: start with letter or underscore, then letters/digits/
# underscores, max 63 chars (the actual Postgres limit).
_IDENTIFIER_RE = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")

# Safe SQL default expressions.  Anything outside this set is rejected to
# prevent SQL injection in the DEFAULT clause.
_SAFE_DEFAULTS: frozenset[str] = frozenset(
    {
        "now()",
        "current_timestamp",
        "gen_random_uuid()",
        "true",
        "false",
        "null",
    }
)
# Simple quoted-string literal or integer/decimal literal.
_LITERAL_DEFAULT_RE = re.compile(r"^'[^';]*'$|^\d+(\.\d+)?$")

# Type names exposed to callers.  Mapping to SQLAlchemy types lives in
# builder.py so this module stays free of SQLAlchemy imports.
ColumnTypeName = Literal[
    "text",
    "integer",
    "bigint",
    "boolean",
    "timestamptz",
    "date",
    "numeric",
    "uuid",
    "jsonb",
]


class ColumnConfig(BaseModel):
    """Declaration of a single user-defined column.

    Args:
        name: Column name — must be a valid lowercase PostgreSQL identifier.
        type: One of the permitted type names.
        nullable: Whether the column accepts NULL.  Defaults to ``True``.
        default: SQL default expression.  Must be one of the safe literals
            or well-known functions.  ``None`` means no default.

    """

    name: Annotated[str, StringConstraints(strip_whitespace=True)]
    type: ColumnTypeName
    nullable: bool = True
    default: str | None = None

    @field_validator("name")
    @classmethod
    def _validate_name(cls, v: str) -> str:
        if not _IDENTIFIER_RE.match(v):
            msg = (
                f"Column name {v!r} is not a valid PostgreSQL identifier. "
                f"Use lowercase letters, digits, and underscores only, "
                f"starting with a letter or underscore."
            )
            raise ValueError(msg)
        return v

    @field_validator("default")
    @classmethod
    def _validate_default(cls, v: str | None) -> str | None:
        if v is None:
            return None
        lowered = v.strip().lower()
        if lowered in _SAFE_DEFAULTS or _LITERAL_DEFAULT_RE.match(v.strip()):
            return v.strip()
        msg = (
            f"Default expression {v!r} is not in the allowlist. "
            f"Permitted values: {sorted(_SAFE_DEFAULTS)}, "
            f"quoted string literals (e.g. 'value'), or numeric literals."
        )
        raise ValueError(msg)


class DimensionConfig(BaseModel):
    """Complete declaration of a user-defined dimension table.

    Args:
        version: Schema version.  Always ``"1"`` for current configs; used
            to detect and migrate older stored configs.
        table_name: Unqualified table name.  The schema is determined by
            the runtime pipeline from the tenant context, not stored here.
        table_type: Factory type.  Currently only ``"simple"`` is supported.
        pk: Primary key strategy.  Defaults to ``"uuidv7"`` for new tables.
        columns: User-defined columns, in declaration order.  Must not
            include a primary key column (that is always added by the
            selected PK plugin).

    """

    version: Literal["1"] = "1"
    table_name: Annotated[str, StringConstraints(strip_whitespace=True)]
    table_type: Literal["simple"] = "simple"
    pk: Literal["serial", "uuidv4", "uuidv7"] = "uuidv7"
    columns: list[ColumnConfig]

    @field_validator("table_name")
    @classmethod
    def _validate_table_name(cls, v: str) -> str:
        if not _IDENTIFIER_RE.match(v):
            msg = (
                f"Table name {v!r} is not a valid PostgreSQL identifier. "
                f"Use lowercase letters, digits, and underscores only, "
                f"starting with a letter or underscore, max 63 characters."
            )
            raise ValueError(msg)
        return v
