"""Naming convention resolution for dimension factories."""

from typing import cast

from sqlalchemy import MetaData


def resolve_name(
    metadata: MetaData,
    key: str,
    substitutions: dict[str, str],
    defaults: dict[str, str],
) -> str:
    """Resolve a name using the metadata naming convention.

    Looks up *key* in ``metadata.naming_convention`` first,
    falling back to *defaults*.

    :param metadata: SQLAlchemy ``MetaData`` with naming convention.
    :param key: Convention key to look up.
    :param substitutions: Values to interpolate into the template.
    :param defaults: Fallback templates when *key* is not in the
        naming convention.
    """
    template = cast(
        "str",
        metadata.naming_convention.get(key, defaults[key]),
    )
    return template % substitutions
