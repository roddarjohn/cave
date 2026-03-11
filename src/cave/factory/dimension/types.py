from pydantic.dataclasses import dataclass


@dataclass
class DimensionConfiguration:
    """Configuration for dimension table factories."""

    id_field_name: str = "id"

    api_schema_name: str = "api"
