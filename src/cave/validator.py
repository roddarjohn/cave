"""Schema item validators for dimension factories."""

from typing import Protocol

from sqlalchemy.schema import SchemaItem

from cave.errors import CaveValidationError


class _SchemaItemValidator(Protocol):
    def __call__(self, item: SchemaItem) -> bool: ...


def is_schema_item_not_primary_key(item: SchemaItem) -> bool:
    """Return True if the item is not a primary key column.

    Args:
        item: The schema item to inspect.

    Returns:
        ``True`` if *item* is not a primary key column.

    """
    return not getattr(item, "primary_key", False)


_default_validators: list[_SchemaItemValidator] = [
    is_schema_item_not_primary_key,
]


def validate_schema_items(
    items: list[SchemaItem],
    *,
    validators: list[_SchemaItemValidator] | None = None,
) -> None:
    """Validate a list of SchemaItems against the given validators.

    Args:
        items: Schema items to validate.
        validators: Validators to run; defaults to
            ``[is_schema_item_not_primary_key]``.

    Raises:
        CaveValidationError: If any item fails a validator.

    """
    validators = validators or _default_validators
    for item in items:
        for validator in validators:
            if not validator(item):
                name = getattr(validator, "__name__", repr(validator))
                msg = f"{name} failed for {item!r}"
                raise CaveValidationError(msg)
