"""PostgREST extension for pgcraft."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from pgcraft.extension import PGCraftExtension
from pgcraft.extensions.postgrest.plugin import PostgRESTPlugin
from pgcraft.extensions.postgrest.view import PostgRESTView

if TYPE_CHECKING:
    from sqlalchemy import MetaData

__all__ = [
    "PostgRESTExtension",
    "PostgRESTPlugin",
    "PostgRESTView",
]


@dataclass
class PostgRESTExtension(PGCraftExtension):
    """Wire PostgREST roles and grants into the pgcraft lifecycle.

    When registered on a :class:`~pgcraft.config.PGCraftConfig`,
    this extension calls
    :func:`~pgcraft.models.roles.register_roles` during
    metadata configuration so that PostgREST roles and
    per-resource grants are emitted by Alembic autogenerate.

    Without this extension, no roles or grants are registered.

    Example::

        from pgcraft.config import PGCraftConfig
        from pgcraft.extensions.postgrest import (
            PostgRESTExtension,
            PostgRESTView,
        )

        config = PGCraftConfig()
        config.use(PostgRESTExtension())

    Args:
        name: Extension name.  Defaults to ``"postgrest"``.
        schema: Default API schema name.  Reserved for future
            use by API view plugins.

    """

    name: str = "postgrest"
    schema: str = "api"

    def configure_metadata(self, metadata: MetaData) -> None:
        """Register PostgREST roles and grants on *metadata*."""
        from pgcraft.models.roles import (  # noqa: PLC0415
            register_roles,
        )

        register_roles(metadata)
