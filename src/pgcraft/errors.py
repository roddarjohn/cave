"""Cave exception types."""


class PGCraftValidationError(Exception):
    """Raised when a pgcraft validation check fails."""


class DestructiveOperationError(PGCraftValidationError):
    """Raised when autogenerate produces a DDL op not in the safety allowlist.

    This typically means the submitted config describes a schema that is
    narrower than the current database schema, which would require destructive
    changes (e.g. dropping a column or table).  The runtime apply pipeline
    never permits such operations.

    Args:
        op: The rejected :class:`~alembic.operations.MigrateOperation`
            instance.

    """

    def __init__(self, op: object) -> None:
        """Store the offending op and build an informative message."""
        self.op = op
        name = type(op).__name__
        super().__init__(
            f"DDL operation {name!r} is not permitted by the runtime safety "
            f"allowlist and was rejected. This usually means the submitted "
            f"config would require removing or altering existing schema "
            f"objects. Only additive operations are allowed at runtime."
        )
