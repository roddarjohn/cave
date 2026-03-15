"""Extension base class and discovery for pgcraft."""

from __future__ import annotations

from dataclasses import dataclass, field
from importlib.metadata import entry_points
from typing import TYPE_CHECKING, ClassVar

from pgcraft.errors import PGCraftValidationError

if TYPE_CHECKING:
    from sqlalchemy import MetaData

    from pgcraft.plugin import Plugin


@dataclass
class Extension:
    """Base class for pgcraft extensions.

    An extension bundles plugins, metadata hooks, Alembic hooks,
    and CLI commands into a single installable unit.

    Subclasses override hook methods to participate in the pgcraft
    lifecycle.  Extensions declare inter-extension dependencies via
    the ``depends_on`` class variable.

    Example::

        @dataclass
        class MyExtension(Extension):
            name: str = "my-ext"

            def plugins(self) -> list[Plugin]:
                return [MyGlobalPlugin()]

            def configure_metadata(self, metadata: MetaData) -> None:
                # register roles, grants, schemas, etc.
                ...

    """

    name: str
    depends_on: ClassVar[list[str]] = field(default=[], init=False, repr=False)

    def plugins(self) -> list[Plugin]:
        """Global plugins prepended to every factory.

        Returns:
            List of plugin instances.  Empty by default.

        """
        return []

    def configure_metadata(self, metadata: MetaData) -> None:
        """Configure metadata-level objects.

        Override to register roles, grants, schemas, or other
        metadata-level objects.  Called by
        ``pgcraft_configure_metadata``.

        Args:
            metadata: The SQLAlchemy ``MetaData`` being configured.

        """

    def configure_alembic(self) -> None:
        """Register custom Alembic renderers or rewriters.

        Override to hook into Alembic setup.  Called by
        ``pgcraft_alembic_hook``.

        """

    def register_cli(self, app: object) -> None:
        """Add subcommands during CLI setup.

        Args:
            app: The ``typer.Typer`` application instance.

        """

    def validate(self, registered_names: frozenset[str]) -> None:
        """Validate after all extensions are loaded.

        Override to check that required peer extensions are
        present or that configuration is consistent.

        Args:
            registered_names: Names of all loaded extensions.

        """


def discover_extensions() -> dict[str, type[Extension]]:
    """Discover extensions via the ``pgcraft.extensions`` entry point group.

    Returns:
        Mapping of extension name to extension class.

    """
    eps = entry_points(group="pgcraft.extensions")
    return {ep.name: ep.load() for ep in eps}


def validate_extension_deps(
    extensions: list[Extension],
) -> None:
    """Check that every extension's ``depends_on`` is satisfied.

    Args:
        extensions: The resolved list of extension instances.

    Raises:
        PGCraftValidationError: If a dependency is missing.

    """
    names = frozenset(ext.name for ext in extensions)
    for ext in extensions:
        deps: list[str] = getattr(type(ext), "depends_on", [])
        missing = [d for d in deps if d not in names]
        if missing:
            msg = (
                f"Extension {ext.name!r} depends on "
                f"{missing!r}, which are not registered."
            )
            raise PGCraftValidationError(msg)
    for ext in extensions:
        ext.validate(names)
