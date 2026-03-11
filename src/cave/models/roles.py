"""PostgREST role and grant declarations."""

from sqlalchemy import MetaData
from sqlalchemy_declarative_extensions import Grants, Role, Roles
from sqlalchemy_declarative_extensions.dialects.postgresql import (
    Grant as PgGrant,
)

from cave.resource import get_api_resources

authenticator = Role("authenticator", in_roles=["anon"])
anon = Role("anon")


def register_roles(metadata: MetaData) -> None:
    """Register PostgREST roles and grants on *metadata*.

    Reads :class:`~cave.models.api.APIResource` objects from the metadata
    and generates per-resource grants.
    """
    metadata.info["roles"] = Roles(ignore_unspecified=True).are(
        authenticator, anon
    )

    resources = get_api_resources(metadata)

    # Collect unique schemas that need USAGE grants
    schemas = {r.schema for r in resources}

    grants = (
        [
            PgGrant.new("usage", to=anon).on_schemas(*schemas),
        ]
        if schemas
        else []
    )
    grants.extend(
        PgGrant.new(*r.grants, to=anon).on_tables(r.qualified_name)
        for r in resources
    )

    metadata.info["grants"] = Grants(ignore_unspecified=True).are(*grants)
