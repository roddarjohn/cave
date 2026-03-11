"""Validators for SQLAlchemy SchemaItems used in dimension factories."""

from typing import Protocol

from sqlalchemy.schema import SchemaItem

from cave.factory.dimension.utils import raise_exception_on_false


class _SchemaItemValidator(Protocol):
    def __call__(self, item: SchemaItem) -> bool: ...


def is_schema_item_not_primary_key(item: SchemaItem) -> bool:
    """Return True if the item is not a primary key column."""
    return not getattr(item, "primary_key", False)


dimension_validators: list[_SchemaItemValidator] = [
    is_schema_item_not_primary_key
]


@raise_exception_on_false
def validate_schema_items(
    items: list[SchemaItem],
    *,
    validators: list[_SchemaItemValidator] | None = None,
) -> bool:
    """Validate a list of SchemaItems against the given validators."""
    validators = validators or dimension_validators

    return all(validator(item) for item in items for validator in validators)
