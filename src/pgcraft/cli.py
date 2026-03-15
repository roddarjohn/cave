from importlib.metadata import version
from typing import Annotated

import typer

from pgcraft.config import PGCraftConfig

app = typer.Typer()

_config: PGCraftConfig | None = None


def configure_cli(config: PGCraftConfig) -> None:
    """Register extensions' CLI commands on the app.

    Call once at startup before ``app()`` to let extensions
    add subcommands.

    Args:
        config: The pgcraft config with extensions.

    """
    global _config  # noqa: PLW0603
    _config = config
    for ext in config._resolved_extensions():  # noqa: SLF001
        ext.register_cli(app)


def _version_callback(value: bool) -> None:  # noqa: FBT001
    """Print version and exit."""
    if value:
        typer.echo(f"pgcraft {version('pgcraft')}")
        raise typer.Exit


@app.callback()
def callback(
    _version: Annotated[  # noqa: FBT002
        bool,
        typer.Option(
            "--version",
            "-V",
            help="Show version and exit.",
            callback=_version_callback,
            is_eager=True,
        ),
    ] = False,
) -> None:
    """Configuration-driven PostgreSQL framework."""


@app.command()
def shoot() -> None:
    """Shoot the portal gun."""
    typer.echo("Shooting portal gun")


@app.command()
def load() -> None:
    """Load the portal gun."""
    typer.echo("Loading portal gun")
