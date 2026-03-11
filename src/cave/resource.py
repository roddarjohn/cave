"""API resource registration for PostgREST grant generation."""

from dataclasses import dataclass, field

from sqlalchemy import MetaData


@dataclass
class APIResource:
    """A database object exposed via PostgREST."""

    name: str
    schema: str = "api"
    grants: list[str] = field(default_factory=lambda: ["select"])

    @property
    def qualified_name(self) -> str:
        """Return the schema-qualified name."""
        return f"{self.schema}.{self.name}"


def register_api_resource(metadata: MetaData, resource: APIResource) -> None:
    """Register an API resource on *metadata*."""
    metadata.info.setdefault("api_resources", []).append(resource)


def get_api_resources(metadata: MetaData) -> list[APIResource]:
    """Return all registered API resources."""
    return metadata.info.get("api_resources", [])
